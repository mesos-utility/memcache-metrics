package main

import (
	"flag"

	"github.com/golang/glog"
	"github.com/mesos-utility/memcache-metrics/cron"
	"github.com/mesos-utility/memcache-metrics/g"
	"github.com/mesos-utility/memcache-metrics/http"
)

var cfg = flag.String("c", "cfg.json", "configuration file")
var version = flag.Bool("version", false, "show version")

func main() {
	defer glog.Flush()
	flag.Parse()

	g.HandleVersion(*version)

	// global config
	g.ParseConfig(*cfg)
	g.InitRpcClients()

	cron.Collect()

	// http
	go http.Start()

	select {}
}
