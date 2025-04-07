# Expressions

- `AssertNotNull` (Runtime Check): This is an active operation. It's a runtime assertion that checks if a value is null.
  If the value is null, `AssertNotNull` throws an exception, stopping the query execution. Think of it as a safeguard.
  It is present in the physical plan. It's there to enforce non-nullity. `AssertNotNull` is not an optimization; it's a
  correctness check.
- `KnownNotNull` (Compile-Time Hint): This is a passive attribute. It's metadata, a hint to the optimizer. It doesn't
  perform any checks. It informs the optimizer that a value will be non-null. `KnownNotNull` is an optimization enabler.
  It's used during logical plan optimization.