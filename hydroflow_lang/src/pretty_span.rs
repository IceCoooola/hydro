//! Pretty, human-readable printing of [`proc_macro2::Span`]s.

/// Helper struct which displays the span as `path:row:col` for human reading/IDE linking.
/// Example: `hydroflow\tests\surface_syntax.rs:42:18`.
pub struct PrettySpan(pub proc_macro2::Span);
impl std::fmt::Display for PrettySpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(feature = "diagnostics")]
        let (path, line, column) = (
            self.0.unwrap().source_file().path(),
            self.0.unwrap().start().line(),
            self.0.unwrap().start().column(),
        );
        #[cfg(feature = "diagnostics")]
        let location = path.display();

        #[cfg(not(feature = "diagnostics"))]
        let (location, line, column) = ("unknown", 0, 0);

        write!(f, "{}:{}:{}", location, line, column)
    }
}

/// Helper struct which displays the span as `row:col` for human reading.
pub struct PrettyRowCol(pub proc_macro2::Span);
impl std::fmt::Display for PrettyRowCol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let span = self.0;
        write!(f, "{}:{}", span.start().line, span.start().column)
    }
}
