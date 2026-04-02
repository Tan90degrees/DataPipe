package commands

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

var (
	serverAddr string
	debugMode  bool
	version    string
)

func NewRootCmd(v string) *cobra.Command {
	version = v

	rootCmd := &cobra.Command{
		Use:   "datapipe",
		Short: "DataPipe CLI - 数据管道命令行工具",
		Long: `DataPipe CLI 是一个用于管理数据管道的命令行工具。

支持以下主要功能：
  - pipeline: 管道管理 (创建、列表、详情、更新、删除、启动、停止、暂停、恢复)
  - function: 函数管理 (注册、列表、详情、删除)
  - execution: 执行管理 (提交、列表、状态、日志)
  - system: 系统管理 (健康检查、配置)
  - version: 版本信息

使用 --server 参数指定 API 服务器地址，默认值为 http://localhost:8080`,
		SilenceUsage: true,
	}

	rootCmd.PersistentFlags().StringVarP(&serverAddr, "server", "s", "http://localhost:8080", "API 服务器地址")
	rootCmd.PersistentFlags().BoolVarP(&debugMode, "debug", "d", false, "启用调试模式")

	rootCmd.AddCommand(
		newPipelineCmd(),
		newFunctionCmd(),
		newExecutionCmd(),
		newSystemCmd(),
		newVersionCmd(),
	)

	return rootCmd
}

func getServerAddr() string {
	return serverAddr
}

func isDebugMode() bool {
	return debugMode
}

func printJSON(v interface{}) {
	if debugMode {
		fmt.Fprintf(os.Stderr, "[DEBUG] Response: %+v\n", v)
	}
}

func printError(cmd *cobra.Command, err error) {
	if debugMode {
		fmt.Fprintf(os.Stderr, "[DEBUG] Error: %v\n", err)
	}
	cmd.PrintErrf("Error: %v\n", err)
	os.Exit(1)
}

func printResponse(cmd *cobra.Command, v interface{}) {
	if debugMode {
		fmt.Fprintf(os.Stderr, "[DEBUG] Response: %+v\n", v)
	}
	fmt.Println(v)
}

func sendRequest(cmd *cobra.Command, method, path string, body interface{}) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("序列化请求体失败: %w", err)
		}
		reqBody = bytes.NewBuffer(data)
		if debugMode {
			fmt.Fprintf(os.Stderr, "[DEBUG] Request Body: %s\n", string(data))
		}
	}

	url := serverAddr + path
	if debugMode {
		fmt.Fprintf(os.Stderr, "[DEBUG] %s %s\n", method, url)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("发送请求失败: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}

	if debugMode {
		fmt.Fprintf(os.Stderr, "[DEBUG] Response Status: %d\n", resp.StatusCode)
		fmt.Fprintf(os.Stderr, "[DEBUG] Response Body: %s\n", string(respBody))
	}

	if resp.StatusCode >= 400 {
		var errResp map[string]interface{}
		if err := json.Unmarshal(respBody, &errResp); err == nil {
			return nil, fmt.Errorf("API 错误: %v", errResp)
		}
		return nil, fmt.Errorf("API 错误 (状态码: %d): %s", resp.StatusCode, string(respBody))
	}

	return respBody, nil
}
