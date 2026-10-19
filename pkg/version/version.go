package version

type Info struct {
	Version string `json:"version"`
	Channel string `json:"channel"`
}

func (i Info) String() string {
	if i.Version != "" {
		return i.Version
	}
	if i.Channel != "" {
		return i.Channel
	}
	return "development"
}

var (
	Version = ""
	Channel = ""
)

func GetInfo() Info {
	return Info{
		Version: Version,
		Channel: Channel,
	}
}
