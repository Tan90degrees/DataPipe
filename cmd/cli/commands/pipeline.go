package commands

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

type NodeDefinition struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Function string                 `json:"function"`
	Config   map[string]interface{} `json:"config"`
}

type EdgeDefinition struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type ExecutionConfig struct {
	Parallelism int `json:"parallelism"`
	RetryPolicy struct {
		MaxRetries int    `json:"maxRetries"`
		Backoff    string `json:"backoff"`
	} `json:"retryPolicy"`
}

type Definition struct {
	Nodes     []NodeDefinition `json:"nodes"`
	Edges     []EdgeDefinition `json:"edges"`
	Execution ExecutionConfig  `json:"execution"`
}

type Pipeline struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Definition  Definition `json:"definition"`
	Version     int        `json:"version"`
	Status      string     `json:"status"`
	CreatedAt   string     `json:"created_at"`
	UpdatedAt   string     `json:"updated_at"`
	CreatedBy   string     `json:"created_by"`
}

type PipelineListResponse struct {
	Data   []Pipeline `json:"data"`
	Total  int64      `json:"total"`
	Offset int        `json:"offset"`
	Limit  int        `json:"limit"`
}

type CreatePipelineRequest struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Definition  Definition `json:"definition"`
	CreatedBy   string     `json:"created_by"`
}

func newPipelineCmd() *cobra.Command {
	pipelineCmd := &cobra.Command{
		Use:   "pipeline",
		Short: "管道管理",
		Long:  `管道管理命令，包括创建、列表、详情、更新、删除、启动、停止、暂停、恢复等操作`,
	}

	pipelineCmd.AddCommand(
		newPipelineCreateCmd(),
		newPipelineListCmd(),
		newPipelineGetCmd(),
		newPipelineUpdateCmd(),
		newPipelineDeleteCmd(),
		newPipelineStartCmd(),
		newPipelineStopCmd(),
		newPipelinePauseCmd(),
		newPipelineResumeCmd(),
	)

	return pipelineCmd
}

func newPipelineCreateCmd() *cobra.Command {
	var name, description, definitionFile string
	var createdBy string

	cmd := &cobra.Command{
		Use:   "create",
		Short: "创建管道",
		Long:  `从文件或参数创建新管道`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var definition Definition
			if definitionFile != "" {
				data, err := os.ReadFile(definitionFile)
				if err != nil {
					return fmt.Errorf("读取定义文件失败: %w", err)
				}
				if err := json.Unmarshal(data, &definition); err != nil {
					return fmt.Errorf("解析定义文件失败: %w", err)
				}
			}

			req := CreatePipelineRequest{
				Name:        name,
				Description: description,
				Definition:  definition,
				CreatedBy:   createdBy,
			}

			resp, err := sendRequest(cmd, http.MethodPost, "/api/v1/pipelines", req)
			if err != nil {
				return err
			}

			fmt.Println("管道创建成功:")
			printResponseJSON(resp)
			return nil
		},
	}

	cmd.Flags().StringVarP(&name, "name", "n", "", "管道名称 (必需)")
	cmd.Flags().StringVarP(&description, "description", "d", "", "管道描述")
	cmd.Flags().StringVarP(&definitionFile, "file", "f", "", "管道定义 JSON 文件路径")
	cmd.Flags().StringVar(&createdBy, "created-by", "", "创建者")
	cmd.MarkFlagRequired("name")

	return cmd
}

func newPipelineListCmd() *cobra.Command {
	var offset, limit int

	cmd := &cobra.Command{
		Use:   "list",
		Short: "列出管道",
		Long:  `列出所有管道，支持分页`,
		RunE: func(cmd *cobra.Command, args []string) error {
			url := fmt.Sprintf("/api/v1/pipelines?offset=%d&limit=%d", offset, limit)
			resp, err := sendRequest(cmd, http.MethodGet, url, nil)
			if err != nil {
				return err
			}

			var result PipelineListResponse
			if err := json.Unmarshal(resp, &result); err != nil {
				return fmt.Errorf("解析响应失败: %w", err)
			}

			fmt.Printf("管道列表 (总数: %d):\n\n", result.Total)
			for _, p := range result.Data {
				fmt.Printf("  ID:       %s\n  Name:     %s\n  Status:   %s\n  Version:  %d\n  Created:  %s\n\n",
					p.ID, p.Name, p.Status, p.Version, p.CreatedAt)
			}
			return nil
		},
	}

	cmd.Flags().IntVar(&offset, "offset", 0, "偏移量")
	cmd.Flags().IntVarP(&limit, "limit", "l", 20, "返回数量")

	return cmd
}

func newPipelineGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get [id]",
		Short: "获取管道详情",
		Long:  `通过 ID 获取管道详细信息`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			url := fmt.Sprintf("/api/v1/pipelines/%s", id)

			resp, err := sendRequest(cmd, http.MethodGet, url, nil)
			if err != nil {
				return err
			}

			fmt.Println("管道详情:")
			printResponseJSON(resp)
			return nil
		},
	}

	return cmd
}

func newPipelineUpdateCmd() *cobra.Command {
	var name, description, definitionFile, changelog string

	cmd := &cobra.Command{
		Use:   "update [id]",
		Short: "更新管道",
		Long:  `更新管道信息，支持版本管理`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]

			req := make(map[string]interface{})
			if name != "" {
				req["name"] = name
			}
			if description != "" {
				req["description"] = description
			}
			if changelog != "" {
				req["changelog"] = changelog
			}
			if definitionFile != "" {
				data, err := os.ReadFile(definitionFile)
				if err != nil {
					return fmt.Errorf("读取定义文件失败: %w", err)
				}
				var definition Definition
				if err := json.Unmarshal(data, &definition); err != nil {
					return fmt.Errorf("解析定义文件失败: %w", err)
				}
				req["definition"] = definition
			}

			url := fmt.Sprintf("/api/v1/pipelines/%s", id)
			resp, err := sendRequest(cmd, http.MethodPut, url, req)
			if err != nil {
				return err
			}

			fmt.Println("管道更新成功:")
			printResponseJSON(resp)
			return nil
		},
	}

	cmd.Flags().StringVarP(&name, "name", "n", "", "管道名称")
	cmd.Flags().StringVarP(&description, "description", "d", "", "管道描述")
	cmd.Flags().StringVarP(&definitionFile, "file", "f", "", "管道定义 JSON 文件路径")
	cmd.Flags().StringVar(&changelog, "changelog", "", "版本变更说明")

	return cmd
}

func newPipelineDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete [id]",
		Short: "删除管道",
		Long:  `通过 ID 删除管道`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			url := fmt.Sprintf("/api/v1/pipelines/%s", id)

			_, err := sendRequest(cmd, http.MethodDelete, url, nil)
			if err != nil {
				return err
			}

			fmt.Println("管道删除成功")
			return nil
		},
	}

	return cmd
}

func newPipelineStartCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start [id]",
		Short: "启动管道",
		Long:  `启动指定 ID 的管道`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			url := fmt.Sprintf("/api/v1/pipelines/%s/start", id)

			resp, err := sendRequest(cmd, http.MethodPost, url, nil)
			if err != nil {
				return err
			}

			printResponseJSON(resp)
			return nil
		},
	}

	return cmd
}

func newPipelineStopCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop [id]",
		Short: "停止管道",
		Long:  `停止指定 ID 的管道`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			url := fmt.Sprintf("/api/v1/pipelines/%s/stop", id)

			resp, err := sendRequest(cmd, http.MethodPost, url, nil)
			if err != nil {
				return err
			}

			printResponseJSON(resp)
			return nil
		},
	}

	return cmd
}

func newPipelinePauseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause [id]",
		Short: "暂停管道",
		Long:  `暂停指定 ID 的管道`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			url := fmt.Sprintf("/api/v1/pipelines/%s/pause", id)

			resp, err := sendRequest(cmd, http.MethodPost, url, nil)
			if err != nil {
				return err
			}

			printResponseJSON(resp)
			return nil
		},
	}

	return cmd
}

func newPipelineResumeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume [id]",
		Short: "恢复管道",
		Long:  `恢复指定 ID 的管道`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			url := fmt.Sprintf("/api/v1/pipelines/%s/resume", id)

			resp, err := sendRequest(cmd, http.MethodPost, url, nil)
			if err != nil {
				return err
			}

			printResponseJSON(resp)
			return nil
		},
	}

	return cmd
}

func printResponseJSON(data []byte) {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		fmt.Println(string(data))
		return
	}

	output, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Println(string(data))
		return
	}
	fmt.Println(string(output))
}
