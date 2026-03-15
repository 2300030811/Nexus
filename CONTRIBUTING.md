# Contributing to Nexus

Thank you for your interest in contributing to the Nexus Real-Time Retail Intelligence platform!

## Development Setup

### Prerequisites
- Python 3.11+
- Docker and Docker Compose
- [Optional] Make

### Setting Up Local Environment

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/Nexus.git
   cd Nexus
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   ```

3. **Install development dependencies:**
   ```bash
   pip install -r requirements-dev.txt
   ```

4. **Start the infrastructure:**
   ```bash
   cd infrastructure
   docker compose up --build
   ```

## Code Quality Standards

We use the following tools to maintain code quality:
- **Ruff**: For linting and formatting.
- **Mypy**: For static type checking.
- **Pytest**: For testing.

Before submitting a PR, please run:
```bash
make lint
make typecheck
make test
```

## Pull Request Process

1. **Create a feature branch** from `develop`.
2. **Commit your changes** with clear, descriptive messages.
3. **Add tests** for any new functionality.
4. **Ensure all checks pass** (Lint, Typecheck, Tests).
5. **Submit a PR** to the `develop` branch for review.

## Architecture Deep Dive

For more details on how the system is built, see [ARCHITECTURE.md](docs/ARCHITECTURE.md).
