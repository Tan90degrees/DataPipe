package commands

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

type HealthResponse struct {
	Status     string            `json:"status"`
	Timestamp string            `json:"timestamp"`
	Components map[string]string `json:"components,omitempty"`
}

type SystemConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func newSystemCmd() *cobra.Command {
	systemCmd := &cobra.Command{
		Use:   "system",
		Short: "系统管理",
		Long:  `系统管理命令，包括健康检查、配置查看等`,
	}

	systemCmd.AddCommand(
		newSystemHealthCmd(),
		newSystemConfigCmd(),
	)

	return systemCmd
}

func newSystemHealthCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "health",
		Short: "健康检查",
		Long:  `检查系统健康状态和组件状态`,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := sendRequest(cmd, http.MethodGet, "/api/v1/system/health", nil)
			if err != nil {
				return err
			}

			var health HealthResponse
			if err := json.Unmarshal(resp, &health); err != nil {
				return fmt.Errorf("解析响应失败: %w", err)
			}

			fmt.Printf("系统状态: %s\n", health.Status)
			if health.Components != nil {
				fmt.Println("\n组件状态:")
				for component, status := range health.Components {
					fmt.Printf("  %s: %s\n", component, status)
				}
			}
			fmt.Printf("\n时间戳: %s\n", health.Timestamp)

			return nil
		},
	}

	return cmd
}

func newSystemConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "获取配置",
		Long:  `获取系统配置信息`,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := sendRequest(cmd, http.MethodGet, "/api/v1/system/config", nil)
			if err != nil {
				return err
			}

			fmt.Println("系统配置:")
			printResponseJSON(resp)
			return nil
		},
	}

	return cmd
}
