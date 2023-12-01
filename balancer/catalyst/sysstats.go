package catalyst

import (
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"time"
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

func GetSystemInfo() (*SystemInfo, error) {
	sysInfo := &SystemInfo{}

	cpuInfo, err := cpu.Info()
	if err != nil {
		return nil, err
	}
	sysInfo.CPUInfo = cpuInfo

	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}
	sysInfo.MemInfo = memInfo

	partitions, err := disk.Partitions(true)
	if err != nil {
		return nil, err
	}

	for _, p := range partitions {
		diskInfo, err := disk.Usage(p.Mountpoint)
		if err != nil {
			return nil, err
		}
		sysInfo.DiskInfo = append(sysInfo.DiskInfo, *diskInfo)
	}

	return sysInfo, nil
}
