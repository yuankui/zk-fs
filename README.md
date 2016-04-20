# zk-fs

## requirements

- golang
- fuse
  - mac: https://osxfuse.github.io/
  - centos: `yum install fuse-devel`

## build


    go get github.com/yuankui/zk-fs
    go install github.com/yuankui/zk-fs
    zk-fs <mount_point> <zk_addr>
