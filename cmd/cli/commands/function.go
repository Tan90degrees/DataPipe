package commands

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

type FunctionDefinition struct {
	InputType  InputType  `json:"input_type"`
	OutputType OutputType `json:"output_type"`
	Config     interface{} `json:"config"`
}

type InputType struct {
	Type   string `json:"type"`
	Schema Schema `json:"schema,omitempty"`
}

type OutputType struct {
	Type   string `json:"type"`
	Schema Schema `json:"schema,omitempty"`
}

type Schema struct {
	Fields []Field `json:"fields"`
}

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Function struct {
	ID         string              `json:"id"`
	Name       string              `json:"name"`
	Type       string              `json:"type"`
	Version    string              `json:"version"`
	Definition FunctionDefinition   `json:"definition"`
	Image      string              `json:"image"`
	Status     string              `json:"status"`
	CreatedAt  string              `json:"created_at"`
	UpdatedAt  string              `json:"updated_at"`
}

type FunctionListResponse struct {
	Data   []Function `json:"data"`
	Total  int64      `json:"total"`
	Offset int        `json:"offset"`
	Limit  int        `json:"limit"`
}

type RegisterFunctionRequest struct {
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Version    string            `json:"version"`
	Definition FunctionDefinition `json:"definition"`
	Image      string            `json:"image"`
}

func newFunctionCmd() *cobra.Command {
	functionCmd := &cobra.Command{
		Use:   "function",
		Short: "函数管理",
		Long:  `函数管理命令，包括注册、列表、详情、删除等操作`,
	}

	functionCmd.AddCommand(
		newFunctionRegisterCmd(),
		newFunctionListCmd(),
		newFunctionGetCmd(),
		newFunctionDeleteCmd(),
	)

	return functionCmd
}

func newFunctionRegisterCmd() *cobra.Command {
	var name, fnType, version, image, definitionFile string
	var inputType, outputType string

	cmd := &cobra.Command{
		Use:   "register",
		Short: "注册函数",
		Long:  `注册新的函数到系统中`,
		RunE: func(cmd *cobra.Command, args []string) error {
			req := RegisterFunctionRequest{
				Name:    name,
				Type:    fnType,
				Version: version,
				Image:   image,
			}

			if definitionFile != "" {
				data, err := os.ReadFile(definitionFile)
				if err != nil {
					return fmt.Errorf("读取定义文件失败: %w", err)
				}
				if err := json.Unmarshal(data, &req.Definition); err != nil {
					return fmt.Errorf("解析定义文件失败: %w", err)
				}
			} else {
				req.Definition = FunctionDefinition{
					InputType:  InputType{Type: inputType},
					OutputType: OutputType{Type: outputType},
				}
			}

			resp, err := sendRequest(cmd, http.MethodPost, "/api/v1/functions", req)
			if err != nil {
				return err
			}

			fmt.Println("函数注册成功:")
			printResponseJSON(resp)
			return nil
		},
	}

	cmd.Flags().StringVarP(&name, "name", "n", "", "函数名称 (必需)")
	cmd.Flags().StringVarP(&fnType, "type", "t", "", "函数类型: start, normal, end (必需)")
	cmd.Flags().StringVarP(&version, "version", "v", "", "函数版本 (必需)")
	cmd.Flags().StringVarP(&image, "image", "i", "", "容器镜像")
	cmd.Flags().StringVarP(&definitionFile, "file", "f", "", "函数定义 JSON 文件路径")
	cmd.Flags().StringVar(&inputType, "input-type", "", "输入类型")
	cmd.Flags().StringVar(&outputType, "output-type", "", "输出类型")
	cmd.MarkFlagRequired("name")
	cmd.MarkFlagRequired("type")
	cmd.MarkFlagRequired("version")

	return cmd
}

func newFunctionListCmd() *cobra.Command {
	var offset, limit int
	var fnType string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "列出函数",
		Long:  `列出所有函数，支持按类型筛选和分页`,
		RunE: func(cmd *cobra.Command, args []string) error {
			url := fmt.Sprintf("/api/v1/functions?offset=%d&limit=%d", offset, limit)
			if fnType != "" {
				url += "&type=" + fnType
			}

			resp, err := sendRequest(cmd, http.MethodGet, url, nil)
			if err != nil {
				return err
			}

			var result FunctionListResponse
			if err := json.Unmarshal(resp, &result); err != nil {
				return fmt.Errorf("解析响应失败: %w", err)
			}

			fmt.Printf("函数列表 (总数: %d):\n\n", result.Total)
			for _, f := range result.Data {
				fmt.Printf("  ID:       %s\n  Name:     %s\n  Type:     %s\n  Version:  %s\n  Status:   %s\n\n",
					f.ID, f.Name, f.Type, f.Version, f.Status)
			}
			return nil
		},
	}

	cmd.Flags().IntVar(&offset, "offset", 0, "偏移量")
	cmd.Flags().IntVarP(&limit, "limit", "l", 20, "返回数量")
	cmd.Flags().StringVar(&fnType, "type", "", "按函数类型筛选 (start, normal, end)")

	return cmd
}

func newFunctionGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get [name]",
		Short: "获取函数详情",
		Long:  `通过名称获取函数详细信息`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			url := fmt.Sprintf("/api/v1/functions/%s", name)

			resp, err := sendRequest(cmd, http.MethodGet, url, nil)
			if err != nil {
				return err
			}

			fmt.Println("函数详情:")
			printResponseJSON(resp)
			return nil
		},
	}

	return cmd
}

func newFunctionDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete [name]",
		Short: "删除函数",
		Long:  `通过名称删除函数`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			url := fmt.Sprintf("/api/v1/functions/%s", name)

			_, err := sendRequest(cmd, http.MethodDelete, url, nil)
			if err != nil {
				return err
			}

			fmt.Println("函数删除成功")
			return nil
		},
	}

	return cmd
}
