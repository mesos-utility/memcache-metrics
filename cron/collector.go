package cron

import (
	"fmt"
	"net"
	"regexp"
	"time"

	"github.com/golang/glog"
	"github.com/mesos-utility/memcache-metrics/g"
	"github.com/open-falcon/common/model"
	memc "github.com/smallfish/memcache"
)

var gaugess = map[string]int{"get_hit_ratio": 1, "incr_hit_ratio": 1,
	"decr_hit_ratio": 1, "delete_hit_ratio": 1, "usage": 1,
	"curr_connections": 1, "total_connections": 1, "bytes": 1,
	"pointer_size": 1, "uptime": 1, "limit_maxbytes": 1, "threads": 1,
	"curr_items": 1, "total_items": 1, "connection_structures": 1}

func Collect() {
	if !g.Config().Transfer.Enable {
		glog.Warningf("Open falcon transfer is not enabled!!!")
		return
	}

	if g.Config().Transfer.Addr == "" {
		glog.Warningf("Open falcon transfer addr is null!!!")
		return
	}

	addrs := g.Config().Daemon.Addrs
	if !g.Config().Daemon.Enable {
		glog.Warningf("Daemon collect not enabled in cfg.json!!!")

		if len(addrs) < 1 {
			glog.Warningf("Not set addrs of daemon in cfg.json!!!")
		}
		return
	}

	collect(addrs)
}

func collect(addrs []string) {
	// start collect data for memcached cluster.
	var attachtags = g.Config().AttachTags
	var interval int64 = g.Config().Transfer.Interval
	var re = regexp.MustCompile("STAT (.*) ([0-9]+\\.?[0-9]*)\n")
	var stats = make(map[string]string)
	var ticker = time.NewTicker(time.Duration(interval) * time.Second)

	for {
	REST:
		<-ticker.C
		hostname, err := g.Hostname()
		if err != nil {
			goto REST
		}

		mvs := []*model.MetricValue{}
		for _, addr := range addrs {
			mc, err := memc.Connect(addr)
			if err != nil {
				glog.Warningf("Error connect for %s", addr)
				continue
			}

			result, err := mc.Stats("")
			if err != nil {
				glog.Warningf("Get %s metrics failed: %s", err)
				continue
			}
			mc.Close()

			segs := re.FindAllStringSubmatch(string(result[:]), -1)

			for i := 0; i < len(segs); i++ {
				key, value := segs[i][1], segs[i][2]
				if key == "" || key == "pid" || key == "time" {
					continue
				} else {
					stats[key] = string(value)
				}
			}

			stats["usage"] = g.CalculateMetricRatio(stats["bytes"], stats["limit_maxbytes"])
			stats["get_hit_ratio"] = g.CalculateMetricRatio(stats["get_hits"], stats["get_misses"])
			stats["incr_hit_ratio"] = g.CalculateMetricRatio(stats["incr_hits"], stats["incr_misses"])
			stats["decr_hit_ratio"] = g.CalculateMetricRatio(stats["decr_hits"], stats["decr_misses"])
			stats["delete_hit_ratio"] = g.CalculateMetricRatio(stats["delete_hits"], stats["delete_misses"])

			var tags string
			_, port, err := net.SplitHostPort(addr)
			if err != nil {
				glog.Warningf("Error format addr of ip:port %s", err)
				continue
			}

			if port != "" {
				tags = fmt.Sprintf("port=%s", port)
			}
			if attachtags != "" {
				tags = fmt.Sprintf("%s,%s", attachtags)
			}

			now := time.Now().Unix()
			var suffix, vtype string
			for k, v := range stats {
				if _, ok := gaugess[k]; ok {
					suffix = ""
					vtype = "GAUGE"
				} else {
					suffix = "_cps"
					vtype = "COUNTER"
				}

				key := fmt.Sprintf("memcached.%s%s", k, suffix)

				metric := &model.MetricValue{
					Endpoint:  hostname,
					Metric:    key,
					Value:     v,
					Timestamp: now,
					Step:      interval,
					Type:      vtype,
					Tags:      tags,
				}

				mvs = append(mvs, metric)
				//glog.Infof("%v\n", metric)
			}
		}
		g.SendMetrics(mvs)
	}
}
