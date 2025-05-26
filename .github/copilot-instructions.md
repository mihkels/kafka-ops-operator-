
# GitHub Copilot Usage Instructions

## Overview
This project welcomes the use of GitHub Copilot to assist with code completion, documentation, and productivity. However, please follow these guidelines to ensure high-quality and maintainable contributions:


## Guidelines

- **Review All Suggestions**: Always review Copilot's code suggestions for correctness, security, and style before committing.
- **Linting**: All Copilot-generated code must pass `golangci-lint` checks. Run `make lint` locally before pushing changes.
- **Formatting**: Use `gofmt` or `goimports` to format your code. CI will fail if code is not properly formatted.
- **Testing**: Write or update unit and E2E tests for all Copilot-assisted features and bug fixes. Ensure all tests pass with `make test` and `make test-e2e`.
- **Documentation**: Add or update documentation for any Copilot-assisted code, especially for public APIs and exported functions. Update `README.md` and relevant docs for any user-facing changes.
- **Security**: Do not accept Copilot suggestions that include hardcoded secrets, credentials, or insecure patterns.
- **Attribution**: If Copilot generates code copied from public sources, ensure it complies with the project's license and attribution requirements.
- **Commits**: Write clear, descriptive commit messages. Group related changes into a single commit when possible.
- **Qodana Static Analysis**: If Qodana is used in CI, ensure Copilot code passes Qodana checks. Run Qodana locally if possible.

## Example: Reviewing and Improving Copilot Suggestions

Below are some examples to illustrate best practices when using Copilot-generated code:

### Example 1: Reviewing and Improving a Function

**Copilot Suggestion:**
```go
func add(a, b int) int { return a + b }
```

**Improved Version:**
```go
// add returns the sum of two integers.
func add(a, b int) int {
    return a + b
}
```

**Why:** Add a comment, use explicit formatting, and follow project style.

### Example 2: Avoiding Hardcoded Secrets

**Bad:**
```go
const apiKey = "my-secret-key"
```

**Good:**
```go
// Load API key from environment variable or secret manager
apiKey := os.Getenv("API_KEY")
```

### Example 3: Adding Tests for Copilot Code

**Copilot Suggestion:**
```go
func multiply(a, b int) int { return a * b }
```

**Test:**
```go
func TestMultiply(t *testing.T) {
    if multiply(2, 3) != 6 {
        t.Error("expected 6")
    }
}
```

**Why:** Always add or update tests for Copilot-assisted code.


## Best Practices

- Use Copilot as a productivity tool, not a replacement for code review or design discussions.
- Always run the full CI pipeline (lint, unit tests, E2E tests, and static analysis) before submitting a pull request.
- If unsure about a suggestion, ask for feedback in your pull request or from a maintainer.
- Regularly update your local Copilot extension to benefit from improvements and bug fixes.
- Avoid accepting Copilot code that introduces code smells, anti-patterns, or reduces maintainability.
- Prefer explicit, readable code over clever or overly concise suggestions.


## Questions or Issues
If you have questions about using Copilot in this project, open an issue or contact a maintainer.
