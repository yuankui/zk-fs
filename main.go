// A Go mirror of libfuse's hello.c

package main

import (
	"log"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"syscall"
	"os/signal"
	"os"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"time"
	"strings"
)

const NODE_FILE string = ".node"

type zkClient struct {
	client *zk.Conn
}

func (this *zkClient) Connect(servers []string) error {
	var err error
	this.client, _, err = zk.Connect(servers, time.Second * 10)
	return err
}

func (this *zkClient) List(path string) ([]string, error) {
	children, _, err := this.client.Children(path)
	children = append(children, NODE_FILE)
	return children, err
}

func (this *zkClient) Get(path string) ([]byte, *zk.Stat, error) {
	bytes, stat, err := this.client.Get(path)
	return bytes, stat, err
}

type ZkFs struct {
	pathfs.FileSystem
	client zkClient
}

func (me *ZkFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	fmt.Println("get attr:", name)
	switch {
	case strings.HasSuffix(name, NODE_FILE):
		var path string = name
		if name == ".node" {
			path = "/"
		} else {
			path = "/" + name[:len(name) - 1 - len(NODE_FILE)]
		}
		_, stat, err := me.client.Get(path)

		var dataLen uint64 = 0

		if err != nil {
			fmt.Println(err)
			dataLen = uint64(0)
		} else {
			dataLen = uint64(stat.DataLength)
		}
		return &fuse.Attr{
			Mode: fuse.S_IFREG | 0644, Size: dataLen,
		}, fuse.OK
	case name == "":
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	default:
		return &fuse.Attr{
			Mode:fuse.S_IFDIR | 0555,
		}, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (me *ZkFs) OpenDir(name string, context *fuse.Context) (c []fuse.DirEntry, code fuse.Status) {
	fmt.Println("open dir:", name)

	children, err := me.client.List("/" + name)

	if err != nil {
		return nil, fuse.EINVAL
	}

	c = []fuse.DirEntry{}

	for _, child := range children {
		c = append(c, fuse.DirEntry{Name: child, Mode: fuse.S_IFDIR})
	}
	code = fuse.OK
	return
}

func (me *ZkFs) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {

	if !strings.HasSuffix(name, "/" + NODE_FILE) {
		return nil, fuse.EINVAL
	}

	// remove suffix /.node
	name = name[:len(name) - len(NODE_FILE) - 1]
	bytes,_, err := me.client.Get("/" + name)
	fmt.Println("getting zk node: ", "/" + name)
	fmt.Println(string(bytes))
	if err != nil {
		fmt.Println(string(bytes))
		fmt.Println(err)
		return nil, fuse.EINVAL
	}


	return nodefs.NewDataFile(bytes), fuse.OK
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage:\n  hello <mount_dir> <zk>")
	}

	var client zkClient = zkClient{}
	fmt.Println(os.Args)
	err := client.Connect(strings.Split(os.Args[2], ","))
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}

	nfs := pathfs.NewPathNodeFs(&ZkFs{FileSystem: pathfs.NewDefaultFileSystem(), client:client}, nil)
	server, _, err := nodefs.MountRoot(os.Args[1], nfs.Root(), nil)

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, os.Kill)
	go func() {
		for {
			s := <-c
			fmt.Println(s)
			err := syscall.Unmount(os.Args[1], 0)
			if err != nil {
				fmt.Println(err)
				continue
			}
			os.Exit(1)
		}
	}()

	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Serve()
}