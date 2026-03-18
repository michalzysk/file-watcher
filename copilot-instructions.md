# Copilot Instructions

## Testing

- Always use **AssertJ** for assertions in tests — never use JUnit's built-in `Assertions` (e.g. `assertEquals`, `assertTrue`) or Hamcrest matchers.
- Import assertions with `import static org.assertj.core.api.Assertions.*;`
- Prefer fluent assertion chains, for example:
  ```java
  assertThat(result).isEqualTo(expected);
  assertThat(list).hasSize(3).contains("foo");
  assertThat(exception).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("invalid");
  ```
- Use `assertThatThrownBy` or `assertThatExceptionOfType` for exception assertions instead of `assertThrows`.
- Use `assertThat(...).as("description")` to add a descriptive message when asserting on non-obvious values.
