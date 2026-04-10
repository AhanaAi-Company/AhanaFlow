# Contributing to AhanaFlow

Thank you for your interest in contributing to AhanaFlow! This document provides guidelines for contributing to the project.

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow

## How to Contribute

### Reporting Bugs

1. Check if the bug has already been reported in [GitHub Issues](https://github.com/AhanaAI-Company/ahanaflow/issues)
2. Create a new issue with:
   - Clear title and description
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment details (OS, Python version, AhanaFlow version)
   - Relevant logs or error messages

### Suggesting Features

1. Check existing feature requests
2. Create an issue tagged as "enhancement"
3. Describe the use case and why it's valuable
4. Provide examples if possible

### Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass (`pytest tests/ -v`)
6. Commit with clear commit messages
7. Push to your fork
8. Open a Pull Request

### Code Standards

- Follow PEP 8 style guide
- Add type hints where applicable
- Document functions and classes with docstrings
- Keep functions focused and small
- Add tests for new features

### Testing

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_state_engine.py -v

# Run with coverage
python -m pytest tests/ --cov=backend --cov-report=html
```

## Development Setup

```bash
# Clone the repo
git clone https://github.com/AhanaAI-Company/ahanaflow.git
cd ahanaflow

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Install in editable mode
pip install -e .

# Run tests
pytest tests/ -v
```

## License

By contributing, you agree that your contributions will be licensed under the same dual license as the project (see [LICENSE](LICENSE)).

## Questions?

Contact us at: dev@ahanaai.com
