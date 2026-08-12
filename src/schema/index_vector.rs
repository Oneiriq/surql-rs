//! Vector-index vocabulary and builders for `DEFINE INDEX`.
//!
//! Split out of [`super::index`] so both modules stay under the repository's
//! 1000-LOC budget. Everything here is re-exported from `schema::index` (and
//! from `schema::table`), so existing paths keep resolving.
//!
//! Covers the distance metrics and element types of the `MTREE`, `HNSW`, and
//! `DISKANN` vector indexes, plus the [`mtree_index`] / [`hnsw_index`] /
//! [`diskann_index`] builder helpers.

use serde::{Deserialize, Serialize};

use super::index::{IndexDefinition, IndexType};

/// Graph out-degree the engine assumes (and echoes) for a `DISKANN` index
/// that never stated `DEGREE`.
pub const DISKANN_DEFAULT_DEGREE: u32 = 64;

/// Build-time candidate list size the engine assumes (and echoes) for a
/// `DISKANN` index that never stated `L_BUILD`.
pub const DISKANN_DEFAULT_L_BUILD: u32 = 100;

/// Pruning slack the engine assumes (and echoes) for a `DISKANN` index that
/// never stated `ALPHA`.
pub const DISKANN_DEFAULT_ALPHA: &str = "1.2";

/// Distance metric for MTREE vector indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MTreeDistanceType {
    /// Cosine distance.
    Cosine,
    /// Euclidean (L2) distance.
    Euclidean,
    /// Manhattan (L1) distance.
    Manhattan,
    /// Minkowski distance.
    Minkowski,
}

impl MTreeDistanceType {
    /// Render as SurrealQL keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cosine => "COSINE",
            Self::Euclidean => "EUCLIDEAN",
            Self::Manhattan => "MANHATTAN",
            Self::Minkowski => "MINKOWSKI",
        }
    }
}

impl std::fmt::Display for MTreeDistanceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Distance metric for HNSW vector indexes (superset of [`MTreeDistanceType`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HnswDistanceType {
    /// Chebyshev distance.
    Chebyshev,
    /// Cosine distance.
    Cosine,
    /// Euclidean distance.
    Euclidean,
    /// Hamming distance.
    Hamming,
    /// Jaccard distance.
    Jaccard,
    /// Manhattan distance.
    Manhattan,
    /// Minkowski distance.
    Minkowski,
    /// Pearson correlation distance.
    Pearson,
}

impl HnswDistanceType {
    /// Render as SurrealQL keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Chebyshev => "CHEBYSHEV",
            Self::Cosine => "COSINE",
            Self::Euclidean => "EUCLIDEAN",
            Self::Hamming => "HAMMING",
            Self::Jaccard => "JACCARD",
            Self::Manhattan => "MANHATTAN",
            Self::Minkowski => "MINKOWSKI",
            Self::Pearson => "PEARSON",
        }
    }
}

impl std::fmt::Display for HnswDistanceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Distance metric for DISKANN vector indexes.
///
/// Deliberately its own enum rather than a reuse of [`HnswDistanceType`]:
/// the engine's DISKANN set both adds metrics HNSW lacks (`INNER_PRODUCT`,
/// `COSINE_NORMALIZED`) and refuses every HNSW metric outside it
/// ("DISKANN supports EUCLIDEAN, COSINE, INNER_PRODUCT, and
/// COSINE_NORMALIZED"), so an out-of-set metric is unrepresentable here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DiskAnnDistanceType {
    /// Cosine distance.
    Cosine,
    /// Cosine distance over pre-normalised vectors.
    CosineNormalized,
    /// Euclidean (L2) distance.
    Euclidean,
    /// Negative inner product.
    InnerProduct,
}

impl DiskAnnDistanceType {
    /// Render as SurrealQL keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cosine => "COSINE",
            Self::CosineNormalized => "COSINE_NORMALIZED",
            Self::Euclidean => "EUCLIDEAN",
            Self::InnerProduct => "INNER_PRODUCT",
        }
    }
}

impl std::fmt::Display for DiskAnnDistanceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Numeric type for vector components in MTREE/HNSW/DISKANN indexes.
///
/// One shared vocabulary; each index kind accepts a subset. The engine takes
/// every variant for HNSW, refuses `F16`/`I8`/`U8` for MTREE, and refuses
/// everything but `F32`/`F16`/`I8`/`U8` for DISKANN —
/// [`IndexDefinition::validate`] teaches those limits before a statement is
/// sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MTreeVectorType {
    /// 64-bit float.
    F64,
    /// 32-bit float.
    F32,
    /// 16-bit float.
    F16,
    /// 64-bit integer.
    I64,
    /// 32-bit integer.
    I32,
    /// 16-bit integer.
    I16,
    /// 8-bit integer.
    I8,
    /// 8-bit unsigned integer.
    U8,
}

impl MTreeVectorType {
    /// Render as SurrealQL keyword.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::F64 => "F64",
            Self::F32 => "F32",
            Self::F16 => "F16",
            Self::I64 => "I64",
            Self::I32 => "I32",
            Self::I16 => "I16",
            Self::I8 => "I8",
            Self::U8 => "U8",
        }
    }
}

impl std::fmt::Display for MTreeVectorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Build an MTREE vector index.
pub fn mtree_index(
    name: impl Into<String>,
    column: impl Into<String>,
    dimension: u32,
    distance: MTreeDistanceType,
    vector_type: MTreeVectorType,
) -> IndexDefinition {
    IndexDefinition {
        index_type: IndexType::Mtree,
        dimension: Some(dimension),
        distance: Some(distance),
        vector_type: Some(vector_type),
        ..IndexDefinition::new(name, [column.into()])
    }
}

/// Build an HNSW vector index.
///
/// `efc` and `m` are optional tuning parameters; when omitted, the server
/// defaults are used.
pub fn hnsw_index(
    name: impl Into<String>,
    column: impl Into<String>,
    dimension: u32,
    distance: HnswDistanceType,
    vector_type: MTreeVectorType,
    efc: Option<u32>,
    m: Option<u32>,
) -> IndexDefinition {
    IndexDefinition {
        index_type: IndexType::Hnsw,
        dimension: Some(dimension),
        vector_type: Some(vector_type),
        hnsw_distance: Some(distance),
        efc,
        m,
        ..IndexDefinition::new(name, [column.into()])
    }
}

/// Build a DISKANN vector index.
///
/// The engine echoes `DEGREE` / `L_BUILD` / `ALPHA` back with defaults filled
/// in even when the definition never stated them, so the builder fills the
/// same defaults up front and the definition compares equal to its own echo.
/// Tune them with [`IndexDefinition::with_degree`] /
/// [`IndexDefinition::with_l_build`] / [`IndexDefinition::with_alpha`], and
/// opt into vector hashing with [`IndexDefinition::with_hashed_vector`].
///
/// ## Examples
///
/// ```
/// use surql::schema::{diskann_index, DiskAnnDistanceType, MTreeVectorType};
///
/// let idx = diskann_index("vec_idx", "v", 3, DiskAnnDistanceType::Cosine, MTreeVectorType::F16);
/// assert_eq!(
///     idx.to_surql("doc"),
///     "DEFINE INDEX vec_idx ON TABLE doc COLUMNS v DISKANN DIMENSION 3 \
///      DIST COSINE TYPE F16 DEGREE 64 L_BUILD 100 ALPHA 1.2;"
/// );
/// ```
pub fn diskann_index(
    name: impl Into<String>,
    column: impl Into<String>,
    dimension: u32,
    distance: DiskAnnDistanceType,
    vector_type: MTreeVectorType,
) -> IndexDefinition {
    IndexDefinition {
        index_type: IndexType::Diskann,
        dimension: Some(dimension),
        diskann_distance: Some(distance),
        vector_type: Some(vector_type),
        degree: Some(DISKANN_DEFAULT_DEGREE),
        l_build: Some(DISKANN_DEFAULT_L_BUILD),
        alpha: Some(DISKANN_DEFAULT_ALPHA.to_string()),
        ..IndexDefinition::new(name, [column.into()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mtree_distance_display() {
        assert_eq!(format!("{}", MTreeDistanceType::Cosine), "COSINE");
    }

    #[test]
    fn hnsw_distance_display() {
        assert_eq!(format!("{}", HnswDistanceType::Chebyshev), "CHEBYSHEV");
    }

    #[test]
    fn diskann_distance_display() {
        assert_eq!(format!("{}", DiskAnnDistanceType::Cosine), "COSINE");
        assert_eq!(
            format!("{}", DiskAnnDistanceType::CosineNormalized),
            "COSINE_NORMALIZED"
        );
        assert_eq!(
            format!("{}", DiskAnnDistanceType::InnerProduct),
            "INNER_PRODUCT"
        );
    }

    #[test]
    fn mtree_vector_type_display() {
        assert_eq!(format!("{}", MTreeVectorType::F32), "F32");
        assert_eq!(format!("{}", MTreeVectorType::F16), "F16");
        assert_eq!(format!("{}", MTreeVectorType::I8), "I8");
        assert_eq!(format!("{}", MTreeVectorType::U8), "U8");
    }

    #[test]
    fn diskann_distance_serde_spells_the_keywords() {
        assert_eq!(
            serde_json::to_string(&DiskAnnDistanceType::CosineNormalized).unwrap(),
            "\"COSINE_NORMALIZED\""
        );
        assert_eq!(
            serde_json::to_string(&DiskAnnDistanceType::InnerProduct).unwrap(),
            "\"INNER_PRODUCT\""
        );
    }

    #[test]
    fn mtree_index_to_surql() {
        let idx = mtree_index(
            "embedding_idx",
            "embedding",
            1536,
            MTreeDistanceType::Cosine,
            MTreeVectorType::F32,
        );
        let sql = idx.to_surql("doc");
        assert!(sql.contains(
            "DEFINE INDEX embedding_idx ON TABLE doc COLUMNS embedding MTREE DIMENSION 1536"
        ));
        assert!(sql.contains("DIST COSINE"));
        assert!(sql.contains("TYPE F32"));
    }

    #[test]
    fn hnsw_index_to_surql_with_efc_m() {
        let idx = hnsw_index(
            "feat_idx",
            "features",
            128,
            HnswDistanceType::Cosine,
            MTreeVectorType::F32,
            Some(500),
            Some(16),
        );
        let sql = idx.to_surql("doc");
        assert!(sql.contains("HNSW DIMENSION 128"));
        assert!(sql.contains("DIST COSINE"));
        assert!(sql.contains("TYPE F32"));
        assert!(sql.contains("EFC 500"));
        assert!(sql.contains("M 16"));
    }

    #[test]
    fn hnsw_index_without_efc_m_omits_them() {
        let idx = hnsw_index(
            "feat_idx",
            "features",
            64,
            HnswDistanceType::Euclidean,
            MTreeVectorType::F64,
            None,
            None,
        );
        let sql = idx.to_surql("doc");
        assert!(!sql.contains("EFC"));
        assert!(!sql.contains("M 12"));
    }

    #[test]
    fn hnsw_index_takes_f16() {
        let idx = hnsw_index(
            "feat_idx",
            "features",
            3,
            HnswDistanceType::Cosine,
            MTreeVectorType::F16,
            None,
            None,
        );
        assert!(idx.to_surql("doc").contains("TYPE F16"));
        assert!(idx.validate().is_ok());
    }

    #[test]
    fn diskann_index_spells_the_full_echo_shape() {
        let idx = diskann_index(
            "vec_idx",
            "v",
            3,
            DiskAnnDistanceType::Cosine,
            MTreeVectorType::F32,
        );
        assert_eq!(
            idx.to_surql("doc"),
            "DEFINE INDEX vec_idx ON TABLE doc COLUMNS v DISKANN DIMENSION 3 \
             DIST COSINE TYPE F32 DEGREE 64 L_BUILD 100 ALPHA 1.2;"
        );
    }

    #[test]
    fn diskann_index_tuned_tail_and_hashed_vector() {
        let idx = diskann_index(
            "vec_idx",
            "v",
            3,
            DiskAnnDistanceType::Cosine,
            MTreeVectorType::F16,
        )
        .with_degree(48)
        .with_l_build(90)
        .with_alpha(1.5)
        .with_hashed_vector(true);
        assert_eq!(
            idx.to_surql("doc"),
            "DEFINE INDEX vec_idx ON TABLE doc COLUMNS v DISKANN DIMENSION 3 \
             DIST COSINE TYPE F16 DEGREE 48 L_BUILD 90 ALPHA 1.5 HASHED_VECTOR;"
        );
    }

    #[test]
    fn diskann_index_composes_with_the_guards_and_concurrently() {
        let idx = diskann_index(
            "vec_idx",
            "v",
            3,
            DiskAnnDistanceType::Euclidean,
            MTreeVectorType::F32,
        )
        .with_concurrently(true);
        assert!(idx.to_surql("doc").ends_with("ALPHA 1.2 CONCURRENTLY;"));
        assert!(idx
            .to_surql_with_options("doc", true)
            .starts_with("DEFINE INDEX IF NOT EXISTS vec_idx"));
        assert!(idx
            .to_surql_overwrite("doc")
            .starts_with("DEFINE INDEX OVERWRITE vec_idx"));
    }
}
