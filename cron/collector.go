package cron

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"time"

	"github.com/golang/glog"
	"github.com/mesos-utility/memcache-metrics/g"
	"github.com/open-falcon/common/model"
	memc "github.com/smallfish/memcache"
)

//var gaugess = [...]string{"get_hit_ratio", "incr_hit_ratio", "decr_hit_ratio",
//	"delete_hit_ratio", "usage", "curr_connections", "total_connections",
//	"bytes", "pointer_size", "uptime", "limit_maxbytes", "threads", "curr_items",
//	"total_items", "connection_structures"}

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

	go collect(addrs)
}

func collect(addrs []string) {
	// start collect data for memcached cluster.
	var attachtags = g.Config().AttachTags

	for {
	REST:
		var interval int64 = g.Config().Transfer.Interval
		time.Sleep(time.Duration(interval) * time.Second)
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
			defer mc.Close()

			result, err := mc.Stats("")
			if err != nil {
				glog.Warningf("Get %s metrics failed: %s", err)
			}

			re := regexp.MustCompile("STAT (.*) ([0-9]+\\.?[0-9]*)\n")
			segs := re.FindAllStringSubmatch(string(result[:]), -1)

			var stats = make(map[string]string)
			for i := 0; i < len(segs); i++ {
				key, value := segs[i][1], segs[i][2]
				if key == "" || key == "pid" || key == "time" {
					continue
				} else {
					stats[key] = string(value)
				}
			}

			bytes, _ := strconv.ParseFloat(stats["bytes"], 64)
			limit_maxbytes, _ := strconv.ParseFloat(stats["limit_maxbytes"], 64)
			stats["usage"] = fmt.Sprintf("%.2f", (100 * bytes / limit_maxbytes))

			if stats["get_hits"] == "0" && stats["get_misses"] == "0" {
				stats["get_hit_ratio"] = "0.0"
			} else {
				get_hits, _ := strconv.ParseFloat(stats["get_hits"], 64)
				get_misses, _ := strconv.ParseFloat(stats["get_misses"], 64)
				stats["get_hit_ratio"] = fmt.Sprintf("%.2f", 100*(get_hits/(get_hits+get_misses)))
			}

			if stats["incr_hits"] == "0" && stats["incr_misses"] == "0" {
				stats["incr_hit_ratio"] = "0.0"
			} else {
				incr_hits, _ := strconv.ParseFloat(stats["incr_hits"], 64)
				incr_misses, _ := strconv.ParseFloat(stats["incr_misses"], 64)
				stats["incr_hit_ratio"] = fmt.Sprintf("%.2f", 100*(incr_hits/(incr_hits+incr_misses)))
			}

			if stats["decr_hits"] == "0" && stats["decr_misses"] == "0" {
				stats["decr_hit_ratio"] = "0.0"
			} else {
				decr_hits, _ := strconv.ParseFloat(stats["decr_hits"], 64)
				decr_misses, _ := strconv.ParseFloat(stats["decr_misses"], 64)
				stats["decr_hit_ratio"] = fmt.Sprintf("%.2f", 100*(decr_hits/(decr_hits+decr_misses)))
			}

			if stats["delete_hits"] == "0" && stats["delete_misses"] == "0" {
				stats["delete_hit_ratio"] = "0.0"
			} else {
				delete_hits, _ := strconv.ParseFloat(stats["delete_hits"], 64)
				delete_misses, _ := strconv.ParseFloat(stats["delete_misses"], 64)
				stats["delete_hit_ratio"] = fmt.Sprintf("%.2f", 100*(delete_hits/(delete_hits+delete_misses)))
			}

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
		g.SendToTransfer(mvs)
	}
}
