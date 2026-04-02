package commands

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "显示版本信息",
		Long:  `显示 DataPipe CLI 的版本信息`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("DataPipe CLI 版本: %s\n", version)
		},
	}

	return cmd
}
