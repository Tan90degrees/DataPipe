package commands

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

type Execution struct {
	ID              string     `json:"id"`
	PipelineID      string     `json:"pipeline_id"`
	PipelineVersion int        `json:"pipeline_version"`
	Status          string     `json:"status"`
	StartTime       *time.Time `json:"start_time,omitempty"`
	EndTime         *time.Time `json:"end_time,omitempty"`
	Progress        int        `json:"progress"`
	ErrorMessage    string     `json:"error_message,omitempty"`
	CreatedAt       string     `json:"created_at"`
}

type Task struct {
	ID           string     `json:"id"`
	ExecutionID  string     `json:"execution_id"`
	NodeID       string     `json:"node_id"`
	FunctionName string     `json:"function_name"`
	Status       string     `json:"status"`
	WorkerID     string     `json:"worker_id,omitempty"`
	StartTime    *time.Time `json:"start_time,omitempty"`
	EndTime      *time.Time `json:"end_time,omitempty"`
	ErrorMessage string     `json:"error_message,omitempty"`
	RetryCount   int        `json:"retry_count"`
	CreatedAt    string     `json:"created_at"`
}

type ExecutionWithTasks struct {
	Execution
	Tasks []Task `json:"tasks,omitempty"`
}

type ExecutionListResponse struct {
	Data   []Execution `json:"data"`
	Total  int64      `json:"total"`
	Offset int        `json:"offset"`
	Limit  int        `json:"limit"`
}

type SubmitExecutionRequest struct {
	PipelineID string                 `json:"pipeline_id"`
	InputData  map[string]interface{} `json:"input_data,omitempty"`
}

type LogEntry struct {
	Timestamp string                 `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	TaskID    string                 `json:"task_id,omitempty"`
	NodeID    string                 `json:"node_id,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

func newExecutionCmd() *cobra.Command {
	executionCmd := &cobra.Command{
		Use:   "execution",
		Short: "执行管理",
		Long:  `执行管理命令，包括提交、列表、状态、日志等操作`,
	}

	executionCmd.AddCommand(
		newExecutionSubmitCmd(),
		newExecutionListCmd(),
		newExecutionStatusCmd(),
		newExecutionLogsCmd(),
	)

	return executionCmd
}

func newExecutionSubmitCmd() *cobra.Command {
	var pipelineID string
	var inputDataFile string

	cmd := &cobra.Command{
		Use:   "submit",
		Short: "提交执行",
		Long:  `提交管道执行任务`,
		RunE: func(cmd *cobra.Command, args []string) error {
			req := SubmitExecutionRequest{
				PipelineID: pipelineID,
			}

			if inputDataFile != "" {
				data, err := os.ReadFile(inputDataFile)
				if err != nil {
					return fmt.Errorf("读取输入数据文件失败: %w", err)
				}
				if err := json.Unmarshal(data, &req.InputData); err != nil {
					return fmt.Errorf("解析输入数据文件失败: %w", err)
				}
			}

			resp, err := sendRequest(cmd, http.MethodPost, "/api/v1/executions", req)
			if err != nil {
				return err
			}

			fmt.Println("执行提交成功:")
			printResponseJSON(resp)
			return nil
		},
	}

	cmd.Flags().StringVarP(&pipelineID, "pipeline-id", "p", "", "管道 ID (必需)")
	cmd.Flags().StringVarP(&inputDataFile, "file", "f", "", "输入数据 JSON 文件路径")
	cmd.MarkFlagRequired("pipeline-id")

	return cmd
}

func newExecutionListCmd() *cobra.Command {
	var offset, limit int
	var pipelineID, status string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "列出执行",
		Long:  `列出所有执行记录，支持按管道 ID 和状态筛选`,
		RunE: func(cmd *cobra.Command, args []string) error {
			url := fmt.Sprintf("/api/v1/executions?offset=%d&limit=%d", offset, limit)
			if pipelineID != "" {
				url += "&pipeline_id=" + pipelineID
			}
			if status != "" {
				url += "&status=" + status
			}

			resp, err := sendRequest(cmd, http.MethodGet, url, nil)
			if err != nil {
				return err
			}

			var result ExecutionListResponse
			if err := json.Unmarshal(resp, &result); err != nil {
				return fmt.Errorf("解析响应失败: %w", err)
			}

			fmt.Printf("执行列表 (总数: %d):\n\n", result.Total)
			for _, e := range result.Data {
				startTime := ""
				if e.StartTime != nil {
					startTime = e.StartTime.Format(time.RFC3339)
				}
				fmt.Printf("  ID:          %s\n  PipelineID:  %s\n  Status:      %s\n  Progress:    %d%%\n  StartTime:   %s\n  CreatedAt:   %s\n\n",
					e.ID, e.PipelineID, e.Status, e.Progress, startTime, e.CreatedAt)
			}
			return nil
		},
	}

	cmd.Flags().IntVar(&offset, "offset", 0, "偏移量")
	cmd.Flags().IntVarP(&limit, "limit", "l", 20, "返回数量")
	cmd.Flags().StringVar(&pipelineID, "pipeline-id", "", "按管道 ID 筛选")
	cmd.Flags().StringVar(&status, "status", "", "按状态筛选 (pending, running, completed, failed, cancelled)")

	return cmd
}

func newExecutionStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status [id]",
		Short: "获取执行状态",
		Long:  `获取指定执行的详细状态信息`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			url := fmt.Sprintf("/api/v1/executions/%s", id)

			resp, err := sendRequest(cmd, http.MethodGet, url, nil)
			if err != nil {
				return err
			}

			fmt.Println("执行详情:")
			printResponseJSON(resp)
			return nil
		},
	}

	return cmd
}

func newExecutionLogsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs [id]",
		Short: "获取执行日志",
		Long:  `获取指定执行的日志信息`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]
			url := fmt.Sprintf("/api/v1/executions/%s/logs", id)

			resp, err := sendRequest(cmd, http.MethodGet, url, nil)
			if err != nil {
				return err
			}

			fmt.Println("执行日志:")
			printResponseJSON(resp)
			return nil
		},
	}

	return cmd
}
