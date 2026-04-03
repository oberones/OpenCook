package version

type Info struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	BuiltAt string `json:"built_at"`
}

var (
	Version = "dev"
	Commit  = "none"
	BuiltAt = "unknown"
)

func Current() Info {
	return Info{
		Version: Version,
		Commit:  Commit,
		BuiltAt: BuiltAt,
	}
}
