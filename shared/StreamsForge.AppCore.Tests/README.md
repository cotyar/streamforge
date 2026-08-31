Tests for the runtime-agnostic core. Listed in **both** solutions, so anything proven here is proven for
both flavours at once — the shape plan 005 established with `PasswordHasher` and plan 015 needs for
`PermissionEvaluator`: an authorization decision that differed between Orleans and Dapr would be a
security bug no single-flavour suite could see.
