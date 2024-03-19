## Capabilities

This service can be used to:

- [ ] stat
- [ ] read
- [ ] write
- [ ] append
- [ ] create_dir
- [ ] delete
- [ ] copy
- [ ] rename
- [ ] list
- [ ] ~~scan~~
- [ ] ~~presign~~
- [ ] blocking

## Configuration

- `root`: Set the work dir for backend.
-
You can refer to [`NucleiFsBuilder`]'s docs for more information

## Example

### Via Builder


```rust
use std::sync::Arc;

use anyhow::Result;
use opendal::services::Fs;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create fs backend builder.
    let mut builder = Fs::default();
    // Set the root for fs, all operations will happen under this root.
    //
    // NOTE: the root must be absolute path.
    builder.root("/tmp");

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::new(builder)?.finish();

    Ok(())
}
```
