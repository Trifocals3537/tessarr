package config

import "testing"

func TestRequiresRestartTreatsRelativeSymlinksAsHot(t *testing.T) {
	current := &Config{}
	updated := &Config{RelativeSymlinks: true}
	if current.RequiresRestart(updated) {
		t.Fatal("relative symlink setting triggered a restart")
	}
}

func TestRequiresRestartAllowsOnlyProvenHotSettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "auth toggle", mutate: func(c *Config) { c.UseAuth = true }},
		{name: "webdav auth", mutate: func(c *Config) { c.EnableWebdavAuth = true }},
		{name: "auth data", mutate: func(c *Config) { c.Auth = &Auth{Username: "operator"} }},
		{name: "arr synchronization", mutate: func(c *Config) { c.Arrs = []Arr{{Name: "sonarr"}} }},
		{name: "repair hook", mutate: func(c *Config) { c.Repair.Enabled = true }},
		{name: "queue cleanup", mutate: func(c *Config) {
			c.QueueCleanup.Rules = []QueueCleanupRule{{Match: "failed", Action: "blacklist"}}
		}},
		{name: "usenet sampling", mutate: func(c *Config) {
			c.Usenet.AvailabilitySamplePercent = 25
			c.Usenet.ImportAvailabilitySamplePercent = 5
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := &Config{}
			updated := &Config{}
			tt.mutate(updated)
			if current.RequiresRestart(updated) {
				t.Fatal("proven hot setting unexpectedly required restart")
			}
		})
	}
}

func TestRequiresRestartDetectsCachedRuntimeSettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "app URL", mutate: func(c *Config) { c.AppURL = "https://media.example" }},
		{name: "download folder", mutate: func(c *Config) { c.DownloadFolder = "/new/downloads" }},
		{name: "refresh interval", mutate: func(c *Config) { c.RefreshInterval = "45s" }},
		{name: "active downloads", mutate: func(c *Config) { c.MaxActiveDownloads = 9 }},
		{name: "job queue capacity", mutate: func(c *Config) { c.JobQueueCapacity = 512 }},
		{name: "retries", mutate: func(c *Config) { c.Retries = 7 }},
		{name: "notifications", mutate: func(c *Config) { c.Notifications.Enabled = true }},
		{name: "STRM", mutate: func(c *Config) { c.Strm.Enabled = true }},
		{name: "file filters", mutate: func(c *Config) { c.AllowedExt = []string{".mkv"} }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := &Config{}
			updated := &Config{}
			tt.mutate(updated)
			if !current.RequiresRestart(updated) {
				t.Fatal("cached setting was incorrectly classified as hot")
			}
		})
	}
}

func TestRequiresRestartIgnoresInactiveMountSettings(t *testing.T) {
	tests := []struct {
		name    string
		current Mount
		updated Mount
	}{
		{
			name:    "no mount",
			current: Mount{Type: MountTypeNone},
			updated: Mount{
				Type:      MountTypeNone,
				MountPath: "/unused",
				DFS:       DFS{CacheDir: "/unused/dfs"},
				Rclone:    Rclone{CacheDir: "/unused/rclone"},
				ExternalRclone: ExternalRclone{
					RCUrl: "http://127.0.0.1:9",
				},
			},
		},
		{
			name:    "legacy empty mount becomes no mount",
			current: Mount{},
			updated: Mount{
				Type:      MountTypeNone,
				MountPath: "/unused",
				DFS:       DFS{CacheDir: "/unused/dfs"},
			},
		},
		{
			name: "dfs ignores rclone settings",
			current: Mount{
				Type:      MountTypeDFS,
				MountPath: "/mnt/tessarr",
				DFS:       DFS{CacheDir: "/cache/dfs"},
			},
			updated: Mount{
				Type:      MountTypeDFS,
				MountPath: "/mnt/tessarr",
				DFS:       DFS{CacheDir: "/cache/dfs"},
				Rclone:    Rclone{CacheDir: "/unused/rclone"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := &Config{Mount: tt.current}
			updated := &Config{Mount: tt.updated}
			if current.RequiresRestart(updated) {
				t.Fatal("inactive mount settings triggered a restart")
			}
		})
	}
}

func TestRequiresRestartDetectsActiveMountChanges(t *testing.T) {
	current := &Config{Mount: Mount{
		Type:      MountTypeDFS,
		MountPath: "/mnt/tessarr",
		DFS:       DFS{CacheDir: "/cache/one"},
	}}
	updated := &Config{Mount: Mount{
		Type:      MountTypeDFS,
		MountPath: "/mnt/tessarr",
		DFS:       DFS{CacheDir: "/cache/two"},
	}}

	if !current.RequiresRestart(updated) {
		t.Fatal("active mount change did not trigger a restart")
	}
}
