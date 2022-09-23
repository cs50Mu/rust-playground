use clap::{Parser, Subcommand};
use futures::stream::TryChunksError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, MigrateError>;

#[derive(Parser)]
#[clap(
    name = "dm",
    bin_name = "dm",
    about = "A tool for migrating product data written in Rust"
)]
pub struct Cli {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// start product migration
    Migrate,
    /// migrate by spu_code
    MigrateBySpu {
        /// spu codes to migrate
        #[clap(value_parser)]
        spu_codes: Vec<String>,
    },
    /// diff data between datamaster and panshi
    Diff,
    /// export empty partners in datamaster
    ExportEmptyPartner,
    /// batch set partner
    BatchSetPartner,
    /// export product
    ExportProduct {
        /// filter used to export, for example: '{"is_virtual": "0"}'
        #[clap(short, long, value_parser)]
        filter: String,
    },
}

#[derive(Error, Debug)]
pub enum MigrateError {
    #[error("database query failed: {0}")]
    SQLError(#[from] sqlx::error::Error),
    #[error("mongodb query failed: {0}")]
    MongodbError(#[from] mongodb::error::Error),
    #[error("{0}")]
    BusinessError(String),
    #[error("parse csv file failed: {0}")]
    CsvError(#[from] csv::Error),
    #[error("collect stream failed: {0}")]
    StreamError(#[from] TryChunksError<Product, mongodb::error::Error>),
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FinCategory {
    pub code: String,
    pub name: String,
    pub stock_cata_code: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Product {
    pub name: String,
    pub code: String,
    pub is_virtual: String,
    pub is_redeem_code: String,
    pub fin_cata_code: String,
    #[serde(skip_deserializing)]
    pub tax_cata_code: String,
    #[serde(skip_deserializing)]
    pub tax_cata_name: String,
    #[serde(skip_deserializing)]
    pub is_verified: String,
    pub receipt_partner_code: String,
    pub deliver_partner_code: String,
    pub remark: String,
    pub skus: Vec<ProductSKU>,
    pub creator: String,
    pub last_modified_by: String,
    pub created: DateTime,
    pub updated: DateTime,
    #[serde(skip_serializing, skip_deserializing)]
    pub clm_product_id: i32,
    #[serde(skip_serializing, skip_deserializing)]
    pub paladin_product_id: i32,
    // company_id: i32,
}

// 要想在mongodb里存入datetime类型，需要使用bson::DateTime类型
// 参考：https://users.rust-lang.org/t/how-to-save-datetime-utc-to-mongos-isodate-in-rust/58674
#[derive(Debug, Serialize, Deserialize)]
pub struct DateTime(pub mongodb::bson::DateTime);

impl Default for DateTime {
    fn default() -> Self {
        Self(mongodb::bson::DateTime::from_millis(0))
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ProductSKU {
    pub name: String,
    pub code: String,
    pub barcode: String,
    pub isbn_code: String,
    // sale_price: BigDecimal,
    // 或者不要存成decimal128类型，因为本身也没有计算的需求，只是展示，用string足够了
    pub sale_price: String,
    pub remark: String,
    pub creator: String,
    pub last_modified_by: String,
    pub is_disable: String,
    pub created: DateTime,
    pub updated: DateTime,
}

impl Product {
    pub fn to_productflatten(&self) -> Vec<ProductFlatten> {
        self.skus
            .iter()
            .map(|sku| ProductFlatten {
                spu_name: self.name.clone(),
                is_virtual: self.is_virtual.clone(),
                is_redeem_code: self.is_redeem_code.clone(),
                fin_cata_code: self.fin_cata_code.clone(),
                sku_name: sku.name.clone(),
                sku_code: sku.code.clone(),
                barcode: {
                    // 双写的逻辑，对于虚拟商品，没有 sync 其 barcode 字段
                    // 在比对的时候特殊处理一下
                    match self.is_virtual.as_str() {
                        "1" => "".to_string(),
                        _ => sku.barcode.clone(),
                    }
                },
                isbn_code: sku.isbn_code.clone(),
                sale_price: sku.sale_price.clone(),
                is_disable: sku.is_disable.clone(),
            })
            .collect()
    }
}

// flattened Product
// mainly used for diff purpose
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ProductFlatten {
    // productSPU related info
    pub spu_name: String,
    pub is_virtual: String,
    pub is_redeem_code: String,
    pub fin_cata_code: String,
    // productSKU related info
    pub sku_name: String,
    pub sku_code: String,
    pub barcode: String,
    pub isbn_code: String,
    pub sale_price: String,
    pub is_disable: String,
}

#[derive(Debug, Deserialize)]
pub struct Partner {
    // 供应商 oa 编码
    pub oa_code: String,
    // 供应商磐石编码
    pub ps_code: String,
    // 供应商名称
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct FinCataCode {
    // 三级财务分类编码
    pub code: String,
}

#[derive(Debug, Deserialize)]
pub struct SkuPartner {
    pub sku_code: String,
    pub partner_code: String,
}

// Conf app config
#[derive(Deserialize, Debug)]
pub struct Conf {
    pub dedao_company_id: i32,
    pub database_url: String,
    pub mongo_url: String,
    pub step_cnt: i32,
    pub process_max_cnt: u32,
    pub progress_step_cnt: u32,
}
