package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type apiClient struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func newClient() *apiClient {
	return &apiClient{
		baseURL: getEnv("SANDBOX_API_URL", "http://localhost:8080"),
		apiKey:  getEnv("SANDBOX_API_KEY", ""),
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *apiClient) do(method, path string, body interface{}) (*http.Response, error) {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.baseURL+path, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	return c.http.Do(req)
}

func (c *apiClient) json(method, path string, body interface{}, out interface{}) error {
	resp, err := c.do(method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("API %d: %s", resp.StatusCode, string(data))
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

var rootCmd = &cobra.Command{
	Use:   "sandbox",
	Short: "Sandbox platform CLI",
}

func main() {
	rootCmd.AddCommand(
		createCmd(),
		listCmd(),
		destroyCmd(),
		pauseCmd(),
		resumeCmd(),
		execCmd(),
		cpCmd(),
		lsCmd(),
		snapshotCmd(),
		checkpointCmd(),
		taskCmd(),
	)
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func createCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new sandbox",
		RunE: func(cmd *cobra.Command, args []string) error {
			template, _ := cmd.Flags().GetString("template")
			memory, _ := cmd.Flags().GetString("memory")
			ttl, _ := cmd.Flags().GetString("ttl")
			isolation, _ := cmd.Flags().GetString("isolation")
			persistent, _ := cmd.Flags().GetBool("persistent")
			disk, _ := cmd.Flags().GetInt("disk")

			c := newClient()
			var result map[string]interface{}
			err := c.json("POST", "/v1/sandboxes", map[string]interface{}{
				"template": template, "memory": memory, "ttl": ttl,
				"isolation": isolation, "persistent": persistent, "disk_gb": disk,
			}, &result)
			if err != nil {
				return err
			}
			fmt.Printf("✓ Sandbox created\n  ID:        %s\n  Status:    %s\n  Isolation: %s\n",
				result["id"], result["status"], result["isolation"])
			return nil
		},
	}
	cmd.Flags().String("template", "python", "Template: python, node, ubuntu, go")
	cmd.Flags().String("memory", "512mb", "Memory limit")
	cmd.Flags().String("ttl", "1h", "Time-to-live")
	cmd.Flags().String("isolation", "standard", "standard or secure (gVisor)")
	cmd.Flags().Bool("persistent", false, "Keep volume between pause/resume")
	cmd.Flags().Int("disk", 0, "Disk size GB for persistent sandboxes")
	return cmd
}

func listCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List sandboxes",
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClient()
			var result map[string]interface{}
			if err := c.json("GET", "/v1/sandboxes", nil, &result); err != nil {
				return err
			}
			sandboxes, _ := result["sandboxes"].([]interface{})
			if len(sandboxes) == 0 {
				fmt.Println("No sandboxes.")
				return nil
			}
			fmt.Printf("%-26s %-12s %-10s %-10s\n", "ID", "STATUS", "TEMPLATE", "ISOLATION")
			fmt.Println(strings.Repeat("─", 62))
			for _, s := range sandboxes {
				sb := s.(map[string]interface{})
				fmt.Printf("%-26s %-12s %-10s %-10s\n",
					sb["id"], sb["status"], sb["template"], sb["isolation"])
			}
			return nil
		},
	}
}

func destroyCmd() *cobra.Command {
	return &cobra.Command{
		Use:  "destroy <id>",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClient()
			resp, err := c.do("DELETE", "/v1/sandboxes/"+args[0], nil)
			if err != nil {
				return err
			}
			resp.Body.Close()
			fmt.Printf("✓ Sandbox %s destroyed\n", args[0])
			return nil
		},
	}
}

func pauseCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "pause <id>",
		Short: "Pause sandbox (stops CPU/memory billing)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClient()
			return c.json("POST", "/v1/sandboxes/"+args[0]+"/pause", nil, nil)
		},
	}
}

func resumeCmd() *cobra.Command {
	return &cobra.Command{
		Use:  "resume <id>",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClient()
			return c.json("POST", "/v1/sandboxes/"+args[0]+"/resume", nil, nil)
		},
	}
}

func execCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "exec <id> -- <command>",
		Short: "Run a command inside a sandbox",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sandboxID := args[0]
			command := strings.Join(args[1:], " ")
			stream, _ := cmd.Flags().GetBool("stream")
			c := newClient()

			if stream {
				resp, err := c.do("GET",
					fmt.Sprintf("/v1/sandboxes/%s/exec/stream?cmd=%s", sandboxID, command), nil)
				if err != nil {
					return err
				}
				defer resp.Body.Close()
				buf := make([]byte, 4096)
				for {
					n, err := resp.Body.Read(buf)
					if n > 0 {
						for _, line := range strings.Split(string(buf[:n]), "\n") {
							if strings.HasPrefix(line, "data: ") {
								var frame map[string]interface{}
								json.Unmarshal([]byte(line[6:]), &frame)
								if t, _ := frame["type"].(string); t == "stdout" {
									fmt.Print(frame["data"])
								}
							}
						}
					}
					if err != nil {
						break
					}
				}
				return nil
			}

			var result map[string]interface{}
			if err := c.json("POST", "/v1/sandboxes/"+sandboxID+"/exec",
				map[string]string{"cmd": command}, &result); err != nil {
				return err
			}
			if s, _ := result["stdout"].(string); s != "" {
				fmt.Print(s)
			}
			if s, _ := result["stderr"].(string); s != "" {
				fmt.Fprint(os.Stderr, s)
			}
			return nil
		},
	}
	cmd.Flags().Bool("stream", false, "Stream output via SSE")
	return cmd
}

func cpCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cp <src> <dst>",
		Short: "Copy files to/from a sandbox  (sandbox-id:/path)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			src, dst := args[0], args[1]
			c := newClient()
			if strings.Contains(src, ":") {
				parts := strings.SplitN(src, ":", 2)
				resp, err := c.do("GET", fmt.Sprintf("/v1/sandboxes/%s/files%s", parts[0], parts[1]), nil)
				if err != nil {
					return err
				}
				defer resp.Body.Close()
				data, _ := io.ReadAll(resp.Body)
				os.WriteFile(dst, data, 0644)
				fmt.Printf("✓ Downloaded %d bytes → %s\n", len(data), dst)
			} else {
				parts := strings.SplitN(dst, ":", 2)
				data, err := os.ReadFile(src)
				if err != nil {
					return err
				}
				req, _ := http.NewRequest("PUT",
					c.baseURL+fmt.Sprintf("/v1/sandboxes/%s/files%s", parts[0], parts[1]),
					bytes.NewReader(data))
				req.Header.Set("Authorization", "Bearer "+c.apiKey)
				resp, _ := c.http.Do(req)
				resp.Body.Close()
				fmt.Printf("✓ Uploaded %d bytes → %s\n", len(data), dst)
			}
			return nil
		},
	}
}

func lsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ls <id> [path]",
		Short: "List files in a sandbox",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := "/workspace"
			if len(args) > 1 {
				path = args[1]
			}
			c := newClient()
			var result map[string]interface{}
			c.json("GET", fmt.Sprintf("/v1/sandboxes/%s/files?path=%s", args[0], path), nil, &result)
			fmt.Println(result["listing"])
			return nil
		},
	}
}

func snapshotCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "snapshots", Short: "Manage snapshots"}
	create := &cobra.Command{
		Use:  "create <id>",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			c := newClient()
			var r map[string]interface{}
			c.json("POST", "/v1/sandboxes/"+args[0]+"/snapshots", map[string]string{"name": name}, &r)
			fmt.Printf("✓ Snapshot %s created\n", r["id"])
			return nil
		},
	}
	create.Flags().String("name", "", "Snapshot name")
	create.MarkFlagRequired("name")
	cmd.AddCommand(create)
	return cmd
}

func checkpointCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "checkpoints", Short: "Manage checkpoints (disk+memory via CRIU)"}

	create := &cobra.Command{
		Use:  "create <id>",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			c := newClient()
			var r map[string]interface{}
			c.json("POST", "/v1/sandboxes/"+args[0]+"/checkpoints", map[string]string{"name": name}, &r)
			fmt.Printf("✓ Checkpoint %s created (disk+memory captured)\n", r["id"])
			return nil
		},
	}
	create.Flags().String("name", "", "Checkpoint name")
	create.MarkFlagRequired("name")

	restore := &cobra.Command{
		Use:   "restore <checkpoint-id>",
		Short: "Restore from checkpoint — resumes mid-execution in ~300ms",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c := newClient()
			var r map[string]interface{}
			c.json("POST", "/v1/checkpoints/"+args[0]+"/restore", nil, &r)
			fmt.Printf("✓ Restored → sandbox %s (%s)\n", r["id"], r["status"])
			return nil
		},
	}

	cmd.AddCommand(create, restore)
	return cmd
}

func taskCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "task", Short: "Manage tasks"}

	trigger := &cobra.Command{
		Use:  "trigger <task-name>",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			wait, _ := cmd.Flags().GetBool("wait")
			c := newClient()
			ep := "/v1/tasks/" + args[0] + "/trigger"
			if wait {
				ep = "/v1/tasks/" + args[0] + "/trigger-and-wait"
			}
			var r map[string]interface{}
			c.json("POST", ep, map[string]interface{}{}, &r)
			fmt.Printf("✓ Run %s — status: %s\n", r["id"], r["status"])
			return nil
		},
	}
	trigger.Flags().Bool("wait", false, "Block until run completes")
	cmd.AddCommand(trigger)
	return cmd
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
