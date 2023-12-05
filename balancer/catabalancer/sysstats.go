package catabalancer

import (
	"fmt"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
)

type LoadAverage struct {
	Load1Min  float64
	Load5Min  float64
	Load15Min float64
}

type SystemUsage struct {
	CPUUsagePercentage float64
	RAMUsagePercentage float64
	BWUsagePercentage  float64
	LoadAvg            LoadAverage
}

type SystemInfo struct {
	CPUInfo  []cpu.InfoStat
	MemInfo  *mem.VirtualMemoryStat
	DiskInfo []disk.UsageStat
}

func GetSystemUsage() (SystemUsage, error) {
	var systemUsage SystemUsage

	// Get CPU usage
	cpuPercents, err := cpu.Percent(time.Second, false)
	if err != nil {
		return systemUsage, err
	}
	if len(cpuPercents) > 0 {
		systemUsage.CPUUsagePercentage = cpuPercents[0]
	} else {
		return SystemUsage{}, fmt.Errorf("cpu usage not found")
	}

	// Get memory usage
	vmStat, err := mem.VirtualMemory()
	if err != nil {
		return systemUsage, err
	}
	systemUsage.RAMUsagePercentage = vmStat.UsedPercent

	// Get Load Average
	avg, err := load.Avg()
	if err != nil {
		return systemUsage, err
	}
	systemUsage.LoadAvg = LoadAverage{
		Load1Min:  avg.Load1,
		Load5Min:  avg.Load5,
		Load15Min: avg.Load15,
	}

	return systemUsage, nil
}
