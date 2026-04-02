package pg

type Status struct {
	Driver     string `json:"driver"`
	Configured bool   `json:"configured"`
	Message    string `json:"message"`
}

type Store struct {
	dsn string
}

func New(dsn string) *Store {
	return &Store{dsn: dsn}
}

func (s *Store) Name() string {
	return "postgres"
}

func (s *Store) Configured() bool {
	return s != nil && s.dsn != ""
}

func (s *Store) Status() Status {
	if !s.Configured() {
		return Status{
			Driver:     "postgres",
			Configured: false,
			Message:    "set OPENCOOK_POSTGRES_DSN to configure persistence",
		}
	}

	return Status{
		Driver:     "postgres",
		Configured: true,
		Message:    "PostgreSQL repository scaffold only",
	}
}

