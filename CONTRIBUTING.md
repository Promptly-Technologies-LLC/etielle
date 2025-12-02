# Contributing to etielle

## Development Environment Setup

### Prerequisites

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) for dependency management
- Docker (for Supabase integration tests)

### Install Dependencies

```bash
# Clone the repository
git clone https://github.com/Promptly-Technologies-LLC/etielle.git
cd etielle

# Install dependencies (including dev dependencies)
uv sync
```

### Running Tests

```bash
# Run all tests
uv run pytest tests/ -v

# Run specific test file
uv run pytest tests/test_transforms.py -v

# Run with coverage
uv run pytest tests/ --cov=etielle
```

### Linting and Type Checking

```bash
# Run linter
uv run ruff check etielle/

# Run type checker
uv run mypy etielle/
```

## Supabase Integration Tests

The Supabase adapter has integration tests that require a running Supabase instance. These tests are skipped by default when the required environment variables are not set.

### Setting Up Local Supabase

1. **Install Supabase CLI**

   ```bash
   # macOS
   brew install supabase/tap/supabase

   # Linux/macOS (via bun - preferred)
   bun install -g supabase

   # Or via npm
   npm install -g supabase

   # Or download from https://github.com/supabase/cli/releases
   ```

2. **Start Local Supabase Stack**

   ```bash
   # Initialize Supabase in the project (first time only)
   npx supabase init

   # Start the local Supabase stack (runs ~10 Docker containers)
   npx supabase start
   ```

   This will output connection details including:
   - API URL: `http://127.0.0.1:54321`
   - Publishable key (e.g., `sb_publishable_...`)
   - Database URL: `postgresql://postgres:postgres@127.0.0.1:54322/postgres`

3. **Create Test Tables**

   Connect to the local Supabase database and create the test tables:

   ```bash
   PGPASSWORD=postgres psql -h 127.0.0.1 -p 54322 -U postgres -d postgres -c "
   CREATE TABLE IF NOT EXISTS test_users (
       id TEXT PRIMARY KEY,
       name TEXT NOT NULL
   );

   CREATE TABLE IF NOT EXISTS test_posts (
       id TEXT PRIMARY KEY,
       title TEXT NOT NULL,
       user_id TEXT REFERENCES test_users(id)
   );

   -- Tables with UUID PKs for two-phase insert tests
   CREATE TABLE IF NOT EXISTS test_orgs (
       id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
       name TEXT NOT NULL UNIQUE
   );

   CREATE TABLE IF NOT EXISTS test_members (
       id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
       name TEXT NOT NULL,
       org_id UUID REFERENCES test_orgs(id)
   );
   "
   ```

   Or use the Supabase Studio at `http://127.0.0.1:54323`.

4. **Set Environment Variables**

   Get the publishable key from `npx supabase status` output:

   ```bash
   export SUPABASE_URL="http://127.0.0.1:54321"
   export SUPABASE_KEY="sb_publishable_..."  # from supabase status
   ```

### Running Supabase Integration Tests

```bash
# Run only Supabase tests
uv run pytest tests/test_supabase_adapter.py -v

# Run all tests (Supabase tests will run if env vars are set)
uv run pytest tests/ -v
```

The integration tests in `TestSupabaseIntegration` class will:
- Clean up test tables before/after each test
- Insert real data to Supabase
- Verify data was persisted correctly

### Stopping Local Supabase

```bash
# Stop the Supabase stack
supabase stop

# Stop and remove all data
supabase stop --no-backup
```

### Troubleshooting

**Tests still skipped after setting env vars:**
- Ensure variables are exported in the current shell
- Check `echo $SUPABASE_URL` returns the expected value

**Connection refused errors:**
- Verify Supabase is running: `supabase status`
- Check Docker containers: `docker ps | grep supabase`

**Table does not exist errors:**
- Create the test tables as shown above
- Verify via Supabase Studio at `http://localhost:54323`

## Code Style

- Use `ruff` for linting
- Follow existing patterns in the codebase
- Add tests for new functionality (TDD preferred)
- Update documentation for user-facing changes

## Pull Request Process

1. Create a feature branch from `main`
2. Make your changes with tests
3. Ensure all tests pass: `uv run pytest tests/ -v`
4. Ensure linting passes: `uv run ruff check etielle/`
5. Update documentation if needed
6. Submit PR with clear description of changes
