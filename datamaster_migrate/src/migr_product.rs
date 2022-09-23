use chrono::Utc;
use clap::Parser;
use config::Config;
use datamaster_migrate::{
    Cli, Commands, Conf, DateTime, FinCataCode, FinCategory, MigrateError, Partner, Product,
    ProductFlatten, ProductSKU, Result, SkuPartner,
};
use env_logger::Env;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use log::{error, warn};
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::{FindOneOptions, FindOptions, UpdateOptions};
use mongodb::{options::ClientOptions, Client, Collection};
use rand::rngs::ThreadRng;
use rand::Rng;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::postgres::Postgres;
use sqlx::Pool;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use umya_spreadsheet::*;

lazy_static! {
    static ref DISABLED_PRODUCT_REGEX: Regex = Regex::new("^.*作废.*$").unwrap();
}

enum MigrateResult {
    AllDone,
    // 按要求处理完指定数量数据，提前返回
    MaxCntReached(u32),
    Processing,
}

// Tasktype 要执行的任务类型
#[allow(dead_code)]
#[derive(Clone, Copy)]
enum TaskType {
    // 迁移商品
    Migrate,
    // 以老服务为基准比对
    DiffOldNew,
    // 以新服务为基准比对
    DiffNewOld,
}

// 新老 partner_code 映射
const PARTNER: &str = include_str!("../fixtures/partner.csv");
// 无需迁移的财务分类
const NO_MIGR_FC: &str = include_str!("../fixtures/no_migr_fc.csv");
// 要更新的 sku 对应的 partner_code
const SKU_PARTNER: &str = include_str!("../fixtures/sku_partner.csv");

fn load_sku_partner(in_file: &str) -> HashMap<String, String> {
    csv::Reader::from_reader(in_file.as_bytes())
        .into_deserialize::<SkuPartner>()
        .map(|p| -> Result<(String, String)> {
            let p = p?;
            Ok((p.sku_code, p.partner_code))
        })
        .collect::<Result<HashMap<String, String>>>()
        .unwrap()
}

fn load_partner(in_file: &str) -> HashMap<String, String> {
    csv::Reader::from_reader(in_file.as_bytes())
        .into_deserialize::<Partner>()
        .map(|p| -> Result<(String, String)> {
            let p = p?;
            Ok((p.ps_code, p.oa_code))
        })
        .collect::<Result<HashMap<String, String>>>()
        .unwrap()
}

fn load_no_migr_fc(in_file: &str) -> HashSet<String> {
    csv::Reader::from_reader(in_file.as_bytes())
        .into_deserialize::<FinCataCode>()
        .map(|fcc| Ok(fcc?.code))
        .collect::<Result<HashSet<String>>>()
        .unwrap()
}

fn transform_fin_catacode(old: &str) -> &str {
    match old {
        "FC0125" | "FC0126" | "FC0128" => "FC0127",
        "FC0119" | "FC0115" => "FC0534",
        _ => old,
    }
}

#[tokio::main]
async fn main() {
    // defaults to log warn and above if the RUST_LOG environment variable isn’t set
    env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();

    let cli = Cli::parse();

    let conf = Config::builder()
        .add_source(config::File::with_name(".conf.toml"))
        .build()
        .unwrap();
    let conf = conf.try_deserialize::<Conf>().unwrap();

    let mut migrate = Migrate::new(&conf).await;

    match cli.command {
        Commands::Migrate => migrate_all(&mut migrate, TaskType::Migrate).await,
        Commands::Diff => migrate_all(&mut migrate, TaskType::DiffOldNew).await,
        Commands::ExportEmptyPartner => {
            migrate.export_empty_partner_records().await.unwrap();
        }
        Commands::BatchSetPartner => {
            migrate.batch_import_partner().await.unwrap();
        }
        Commands::ExportProduct { filter } => {
            migrate.export_product_excel(filter.as_str()).await.unwrap();
        }
        Commands::MigrateBySpu { spu_codes } => {
            println!("migrating spu_codes: {spu_codes:?}");
            migrate
                .do_migrate(TaskType::Migrate, Some(spu_codes))
                .await
                .unwrap();
        }
    };
}

async fn migrate_all(migrate: &mut Migrate, task_type: TaskType) {
    loop {
        match migrate.do_migrate(task_type, None).await {
            Ok(res) => match res {
                MigrateResult::AllDone => {
                    warn!("all done!");
                    break;
                }
                MigrateResult::MaxCntReached(cnt) => {
                    warn!("max_cnt: {} reached!", cnt);
                    break;
                }
                MigrateResult::Processing => {}
            },
            Err(err) => {
                error!("{}", err);
            }
        }
    }
}

struct Migrate {
    pg_pool: Pool<Postgres>,
    dedao_company_id: i32,
    step_cnt: i32,
    maxid_now: i32,
    processed: u32,
    process_max_cnt: u32,
    progress_step_cnt: u32,
    // 当前实际查询到的记录数
    select_resp_cnt: i32,
    partner_map: HashMap<String, String>,
    rng: ThreadRng,
    // 无需迁移的财务分类
    no_migr_fc: HashSet<String>,
    sku_partner: HashMap<String, String>,
    product_collection: Collection<Product>,
    fin_category: Collection<FinCategory>,
}

impl<'a> Migrate {
    async fn new(conf: &Conf) -> Self {
        // pg conn
        let pg_pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&conf.database_url)
            .await
            .unwrap();

        // mongodb
        let client_options = ClientOptions::parse(&conf.mongo_url).await.unwrap();
        let client = Client::with_options(client_options).unwrap();
        let db = client.database("datamaster");
        let product_coll = db.collection::<Product>("product");
        let fin_category_coll = db.collection::<FinCategory>("finance_category");

        Self {
            dedao_company_id: conf.dedao_company_id,
            step_cnt: conf.step_cnt,
            process_max_cnt: conf.process_max_cnt,
            progress_step_cnt: conf.progress_step_cnt,
            pg_pool,
            rng: rand::thread_rng(),
            maxid_now: 0,
            processed: 0,
            select_resp_cnt: 0,
            partner_map: load_partner(PARTNER),
            no_migr_fc: load_no_migr_fc(NO_MIGR_FC),
            sku_partner: load_sku_partner(SKU_PARTNER),
            product_collection: product_coll,
            fin_category: fin_category_coll,
        }
    }

    async fn export_product_excel(&self, filter: &str) -> Result<()> {
        // string -> bson::Document
        let value: Document = serde_json::from_str(filter).unwrap();
        let bson = Bson::try_from(value).unwrap();
        let filter = bson.as_document().unwrap();

        // fetch data from mongoDB
        // by default, cursor will timeout in a while(like 10 minutes)
        let find_options = FindOptions::builder().no_cursor_timeout(true).build();
        let mut cursor = self
            .product_collection
            .find(filter.clone(), find_options)
            .await?;

        let mut book = new_file();
        // book.new_sheet("Sheet1").unwrap();
        let ws = book.get_sheet_by_name_mut("Sheet1").unwrap();

        // write sheet header
        let header = vec![
            r#""1baseinfo_$head,invcode,invname,pk_invcl,pk_measdoc,pk_taxitems,invbarcode,invspec""#,
            "存货编码",
            "存货名称",
            "存货分类",
            "主计量单位",
            "税目",
            "条形码",
            "规格",
        ];
        header.into_iter().enumerate().for_each(|(idx, val)| {
            ws.get_cell_by_column_and_row_mut(&((idx + 1) as u32), &1)
                .set_value_from_string(val);
        });

        // data start from 2, header is 1
        let mut row_idx = 2;
        while let Some(product) = cursor.try_next().await? {
            // if row_idx > 10 {
            //     break;
            // }
            // println!("{:?}", product);

            let filter = doc! { "code": product.fin_cata_code };
            if let Some(fin_cata) = self
                .fin_category
                .find_one(filter, FindOneOptions::default())
                .await?
            {
                if !fin_cata.stock_cata_code.is_empty() {
                    for sku in product.skus {
                        // id start from 1
                        let row = vec![
                            (row_idx - 1).to_string(),
                            sku.code,
                            sku.name,
                            fin_cata.stock_cata_code.clone(),
                            "0000002".into(),
                            "4004".into(),
                            sku.barcode,
                            sku.isbn_code,
                        ];
                        row.into_iter().enumerate().for_each(|(idx, val)| {
                            ws.get_cell_by_column_and_row_mut(&((idx + 1) as u32), &row_idx)
                                .set_value_from_string(val);
                        });
                        row_idx += 1;
                    }
                }
            };
        }

        // gen excel file
        let path = std::path::Path::new("./umya.xlsx");
        writer::xlsx::write(&book, path).unwrap();

        Ok(())
    }

    // TODO: 可以尝试下批量 update，单个更新确实较慢
    // input: sku_code -> partner_code
    async fn batch_import_partner(&self) -> Result<()> {
        for (sku_code, partner_code) in &self.sku_partner {
            let filter = doc! { "skus.code": sku_code };
            let update = doc! { "$set": { "receipt_partner_code": partner_code.clone(), "deliver_partner_code": partner_code } };
            self.product_collection
                .update_one(filter, update, UpdateOptions::default())
                .await?;
        }

        Ok(())
    }

    async fn export_empty_partner_records(&self) -> Result<()> {
        let filter = doc! { "deliver_partner_code": "" };
        // by default, cursor will timeout in a while(like 10 minutes)
        let find_options = FindOptions::builder().no_cursor_timeout(true).build();
        let cursor = self.product_collection.find(filter, find_options).await?;
        let products: Vec<_> = cursor.try_collect().await?;

        let file = File::create("./empty_partners.csv")?;
        let mut wtr = csv::Writer::from_writer(file);
        let products_flatten = products.into_iter().flat_map(|p| p.to_productflatten());

        for pf in products_flatten {
            wtr.serialize(pf)?;
        }

        wtr.flush()?;

        Ok(())
    }

    async fn fetch_products(&mut self, spu_codes: Option<Vec<String>>) -> Result<Vec<Product>> {
        #[derive(Debug, Serialize, Deserialize)]
        struct PsProduct {
            clm_product_id: i32,
            paladin_product_id: i32,
            creator: Option<String>,
            product_name: Option<String>,
            invoice_category: Option<String>,
            revenue_category_code: Option<String>,
            created: chrono::DateTime<Utc>,
            updated: chrono::DateTime<Utc>,
            is_physical: Option<bool>,
        }

        let panshi_products = {
            if let Some(spu_codes) = spu_codes {
                sqlx::query_as!(
                    PsProduct,
                    r#"
                        SELECT cp.id as clm_product_id,
                                cp.pan_product_id as paladin_product_id,
                                ca.username AS "creator?",
                                cp.product_name,
                                cp.invoice_category,
                                cp.revenue_category_code,
                                cp.created,
                                cp.updated,
                                pp.is_physical as "is_physical?"
                        FROM clm.partner_product cp
                        LEFT JOIN paladin.light_product pp ON cp.pan_product_id = pp.id
                        LEFT JOIN clm.auth_user ca ON cp.creator_id = ca.id
                        WHERE cp.company_id = $1 AND cp.product_id = ANY($2);
                    "#,
                    self.dedao_company_id,
                    &spu_codes,
                )
                .fetch_all(&self.pg_pool)
                .await?
            } else {
                sqlx::query_as!(
                    PsProduct,
                    r#"
                SELECT cp.id as clm_product_id,
                        cp.pan_product_id as paladin_product_id,
                        ca.username AS "creator?",
                        cp.product_name,
                        cp.invoice_category,
                        cp.revenue_category_code,
                        cp.created,
                        cp.updated,
                        pp.is_physical as "is_physical?"
                FROM clm.partner_product cp
                LEFT JOIN paladin.light_product pp ON cp.pan_product_id = pp.id
                LEFT JOIN clm.auth_user ca ON cp.creator_id = ca.id
                WHERE cp.company_id = $1
                AND cp.id > $2
                ORDER BY cp.id
                LIMIT $3;
            "#,
                    self.dedao_company_id,
                    self.maxid_now,
                    self.step_cnt as i64
                )
                .fetch_all(&self.pg_pool)
                .await?
            }
        };

        self.select_resp_cnt = panshi_products.len() as i32;
        if self.select_resp_cnt > 0 {
            self.maxid_now = panshi_products[(self.select_resp_cnt - 1) as usize].clm_product_id;
            self.processed += self.select_resp_cnt as u32;
        }

        let dm_products = panshi_products
            .into_iter()
            .filter_map(|product| -> Option<Product> {
                if product.creator.is_none() {
                    warn!("[clm_product:{}]no related creator", product.clm_product_id);
                    return None;
                }
                let creator = product.creator.unwrap();
                let creator = self.normalize_creator(&creator);
                if product.is_physical.is_none() {
                    warn!(
                        "[clm_product:{}]no related paladin product",
                        product.clm_product_id
                    );
                    return None;
                }
                let rand_num = self.rng.gen_range(0..=999999999999_u64);
                Some(Product {
                    // code: Uuid::new_v4().to_string(),
                    // PDH + 12 位随机数字
                    code: format!("PDH{:012}", rand_num),
                    name: product.product_name.unwrap_or_default(),
                    creator: creator.to_string(),
                    last_modified_by: creator.to_string(),
                    is_virtual: if product.is_physical.unwrap() {
                        "0".to_string()
                    } else {
                        "1".to_string()
                    },
                    tax_cata_code: product.revenue_category_code.unwrap_or_default(),
                    tax_cata_name: product.invoice_category.unwrap_or_default(),
                    is_verified: "1".to_string(),
                    created: DateTime(product.created.into()),
                    updated: DateTime(product.updated.into()),
                    clm_product_id: product.clm_product_id,
                    paladin_product_id: product.paladin_product_id,
                    ..Product::default()
                })
            })
            .collect::<Vec<_>>();

        Ok(dm_products)
    }

    fn normalize_creator<'b>(&self, panshi_creator: &'b str) -> &'b str {
        // - 所有不带「@」符号的，直接复制创建人名称
        // - 「hepeng@luojilab.com」 截断为hepeng
        // - 所有「数字_bot@luojilab.com」的，统一为api_create（未来所有外部系统对接调用创建的，都记为api_create）
        match panshi_creator {
            // match guard
            // ref: https://stackoverflow.com/questions/43211180/is-there-a-way-to-use-custom-patterns-such-as-a-regex-or-functions-in-a-match
            s if s.contains('@') => match s {
                "hepeng@luojilab.com" => "hepeng",
                _ => "api_create",
            },
            s => s,
        }
    }

    async fn is_card(&self, paladin_product_id: i32) -> Result<String> {
        let is_card = sqlx::query!(
            r#"
                SELECT le.value
                FROM paladin.light_productext le
                JOIN paladin.light_product lp ON le.product_id = lp.id
                WHERE lp.id = $1
                AND le.prop = 'is_card';
            "#,
            paladin_product_id
        )
        .fetch_optional(&self.pg_pool)
        .await?;

        Ok(match is_card {
            None => "0".to_string(),
            Some(is_card) => is_card.value,
        })
    }

    async fn fin_cata_code(&self, paladin_product_id: i32) -> Result<String> {
        let fin_cata_id = sqlx::query!(
            r#"
                SELECT le.value
                FROM paladin.light_productext le
                JOIN paladin.light_product lp ON le.product_id = lp.id
                WHERE lp.id = $1
                AND le.prop = 'finance_category_id';
            "#,
            paladin_product_id
        )
        .fetch_optional(&self.pg_pool)
        .await?;
        let fin_cata_id = fin_cata_id.ok_or_else(|| {
            MigrateError::BusinessError(format!(
                "[paladin_product:{}]fin_cata_id not found",
                paladin_product_id
            ))
        })?;
        let fin_cata_id: i32 = fin_cata_id.value.parse().map_err(|_| {
            MigrateError::BusinessError(format!(
                "[paladin_product:{}]fin_cata_id is not a number: {:?}",
                paladin_product_id, fin_cata_id.value
            ))
        })?;
        let fin_cata_code = sqlx::query!(
            r#"
                SELECT code
                FROM paladin.light_category
                WHERE id = $1;
            "#,
            fin_cata_id
        )
        .fetch_one(&self.pg_pool)
        .await?;
        Ok(fin_cata_code.code)
    }

    // get partner codes by sku_codes
    async fn partner_codes(&self, sku_codes: Vec<String>) -> Result<HashMap<String, String>> {
        let records = sqlx::query!(
            r#"
                SELECT 
                distinct cci.sku_code, 
                FIRST_VALUE(pp.partner_id) over (
                    partition by cci.sku_code 
                    order by 
                    ce.time_expire desc
                ) as partner_id 
                FROM 
                clm.contract_contract AS cc 
                JOIN clm.contract_contractitem AS cci ON cc.id = cci.contract_id 
                JOIN clm.contract_party AS cp ON cc.id = cp.contract_id 
                right JOIN clm.partner_partner AS pp ON cp.partner_id = pp.partner_id 
                JOIN clm.contract_expirydate AS ce ON cc.expiry_date_id = ce.id 
                WHERE 
                cc.contract_type = 1 
                AND cp.party_side = 'a' 
                AND cci.sku_code = ANY($1)
            "#,
            &sku_codes
        )
        .fetch_all(&self.pg_pool)
        .await?;

        Ok(records
            .into_iter()
            .map(|record| {
                let (sku_code, partner_id) = (record.sku_code.unwrap(), record.partner_id.unwrap());
                let oa_partner = self.get_oa_partner(&partner_id, &sku_code);
                (sku_code, oa_partner)
            })
            .collect())
    }

    // 新老 partner 的映射
    fn get_oa_partner(&self, partner_id: &str, sku_code: &str) -> String {
        let oa_partner = self.partner_map.get(partner_id);
        if let Some(partner) = oa_partner {
            partner.to_owned()
        } else {
            warn!(
                "[sku_code:{}] no matching oa_partner for ps_partner: {}",
                sku_code, partner_id
            );
            "".to_string()
        }
    }

    async fn skus(&self, clm_product_id: i32, creator: &str) -> Result<Vec<ProductSKU>> {
        let skus = sqlx::query!(
            r#"
                SELECT cp.sku_code,
                    cp.sku_name,
                    cp.description,
                    pl.fixed_price AS "fixed_price?",
                    pp.barcode AS "barcode?",
                    cp.created,
                    cp.updated,
                    pp.isbn AS "isbn?"
                FROM clm.partner_productitem cp
                LEFT JOIN paladin.light_sku pl ON cp.pan_sku_id = pl.id
                LEFT JOIN paladin.light_product pp on pl.product_id = pp.id 
                WHERE cp.product_id = $1;
            "#,
            clm_product_id
        )
        .fetch_all(&self.pg_pool)
        .await?;

        if skus.is_empty() {
            return Err(MigrateError::BusinessError(format!(
                "[clm_product:{}]no related sku",
                clm_product_id
            )));
        }

        // -- sku_name,
        // -- sku_code,
        // -- remark,
        // -- sale_price // cp.product_id 为 clm.partner_product 的主键id
        skus.iter()
            .map(|sku| -> Result<ProductSKU> {
                if sku.fixed_price.is_none() {
                    return Err(MigrateError::BusinessError(format!(
                        "[clm_product:{}]no related paladin sku",
                        clm_product_id
                    )));
                }
                let fixed_price = sku.fixed_price.as_ref().unwrap();
                if sku.barcode.is_none() {
                    return Err(MigrateError::BusinessError(format!(
                        "[clm_product:{}](barcode)no related paladin product",
                        clm_product_id
                    )));
                }
                let barcode = sku.barcode.as_ref().unwrap();
                if sku.isbn.is_none() {
                    return Err(MigrateError::BusinessError(format!(
                        "[clm_product:{}](isbn)no related paladin product",
                        clm_product_id
                    )));
                }
                let isbn = sku.isbn.as_ref().unwrap();

                Ok(ProductSKU {
                    name: sku.sku_name.to_string(),
                    code: sku.sku_code.to_string(),
                    // as_ref converts from &Option<T> to Option<&T>
                    // map converts str to String
                    remark: sku
                        .description
                        .as_ref()
                        .map(|x| x.to_string())
                        .unwrap_or_default(),
                    sale_price: format!("{:.2}", fixed_price),
                    barcode: barcode.to_string(),
                    isbn_code: isbn.to_string(),
                    creator: creator.to_string(),
                    last_modified_by: creator.to_string(),
                    is_disable: if DISABLED_PRODUCT_REGEX.is_match(&sku.sku_name) {
                        "1".to_string()
                    } else {
                        "0".to_string()
                    },
                    created: DateTime(sku.created.into()),
                    updated: DateTime(sku.updated.into()),
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    async fn fill_empty_fields(&mut self, product: &mut Product) -> Result<()> {
        (product.is_redeem_code, product.fin_cata_code, product.skus) = tokio::try_join!(
            self.is_card(product.paladin_product_id),
            self.fin_cata_code(product.paladin_product_id),
            self.skus(product.clm_product_id, &product.creator),
        )?;

        Ok(())
    }

    async fn do_migrate(
        &mut self,
        task_type: TaskType,
        spu_codes: Option<Vec<String>>,
    ) -> Result<MigrateResult> {
        let mut products = self.fetch_products(spu_codes).await?;

        let mut need_remove = Vec::new();
        for (idx, product) in &mut products.iter_mut().enumerate() {
            // if business err, skip this product and continue
            match self.fill_empty_fields(product).await {
                Ok(_) => {
                    // 处理无需迁移的、需要转换的财务分类
                    if self.no_migr_fc.contains(&product.fin_cata_code) {
                        need_remove.push(idx);
                    } else {
                        product.fin_cata_code =
                            transform_fin_catacode(&product.fin_cata_code).to_string();
                    }
                }
                // 如此操作尚存在一个问题：有问题的数据依然会被不完整的插入mongo中
                // 若能使得问题数据完全不被插入会比较好
                // 一个思路：在此处记下idx，然后在循环完成后再根据idx删除？
                Err(MigrateError::BusinessError(err_msg)) => {
                    warn!("{}", err_msg);
                    need_remove.push(idx);
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        // 用 swap_remove 的话，需要保证是从后往前删
        // 否则可能会 panic
        // 参考：https://stackoverflow.com/questions/57947441/remove-a-sequence-of-values-from-a-vec-in-rust
        // 以及 `Vector::swap_remove`
        need_remove.reverse();
        need_remove
            .iter()
            .copied()
            .map(|idx| products.swap_remove(idx))
            // 不关心结果，只需要side effect时可以这么做
            .for_each(drop);

        self.batch_set_partner(&mut products).await?;

        match task_type {
            TaskType::Migrate => {
                if !products.is_empty() {
                    match self.product_collection.insert_many(&products, None).await {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("Error inserting: {:?}", products);
                            return Err(e.into());
                        }
                    }
                }
            }
            TaskType::DiffOldNew => {
                self.diff_old_new(products).await?;
            }
            TaskType::DiffNewOld => todo!(),
        }

        if self.processed % self.progress_step_cnt == 0 {
            warn!("processed {} elems", self.processed);
        }

        if self.process_max_cnt > 0 && self.processed >= self.process_max_cnt {
            Ok(MigrateResult::MaxCntReached(self.processed))
        } else if self.select_resp_cnt < self.step_cnt {
            Ok(MigrateResult::AllDone)
        } else {
            Ok(MigrateResult::Processing)
        }
    }

    async fn batch_set_partner(&mut self, products: &mut [Product]) -> Result<()> {
        let sku_codes = products
            .iter()
            .flat_map(|p| p.skus.iter().map(|sku| sku.code.clone()))
            .collect::<Vec<String>>();

        let sku_partner = self.partner_codes(sku_codes).await?;

        products.iter_mut().for_each(|p| {
            // every product should have one partner
            let partner_code = {
                let sku_codes = p
                    .skus
                    .iter()
                    .map(|sku| sku_partner.get(&sku.code))
                    .collect::<Vec<Option<_>>>();
                // all equal
                if is_all_equal(&sku_codes) {
                    // if all are None, should return empty string
                    if sku_codes[0].is_none() {
                        warn!("[dm_spucode:{}] partner_id not found", p.code);
                        "".to_string()
                    } else {
                        sku_codes[0].unwrap().clone()
                    }
                } else {
                    // if not equal, return empty string, too
                    warn!(
                        "[dm_spucode:{}] mutiple sku with different partners",
                        p.code
                    );
                    "".to_string()
                }
            };

            p.receipt_partner_code = partner_code.clone();
            p.deliver_partner_code = partner_code;
        });

        Ok(())
    }

    // diff_old_new 以老服务为基准
    async fn diff_old_new(&mut self, products: Vec<Product>) -> Result<()> {
        // Vec<Product> --> Vec<ProductFlatten>
        let ps_products_flatten: HashSet<ProductFlatten> = products
            .iter()
            .flat_map(|product| product.to_productflatten())
            .collect();

        let sku_codes: Vec<String> = ps_products_flatten
            .iter()
            .map(|product| product.sku_code.clone())
            .collect();

        let filter = doc! { "skus.code": { "$in": sku_codes } };
        let cursor = self
            .product_collection
            .find(filter, FindOptions::default())
            .await?;

        let dm_products: Vec<Product> = cursor.try_collect().await?;

        let dm_products_flatten: HashSet<ProductFlatten> = dm_products
            .iter()
            .flat_map(|product| product.to_productflatten())
            .collect();

        // in ps not in dm
        let ps_dm: Vec<ProductFlatten> = ps_products_flatten
            .difference(&dm_products_flatten)
            .cloned()
            .collect();

        ps_dm.iter().for_each(|p| {
            error!("in ps not in dm: {p:?}");
        });

        // in dm not in ps
        let dm_ps: Vec<ProductFlatten> = dm_products_flatten
            .difference(&ps_products_flatten)
            .cloned()
            .collect();

        dm_ps.iter().for_each(|p| {
            error!("in dm not in ps: {p:?}");
        });

        Ok(())
    }
}

// is_all_equal check if all elements of the vec are equal
// refer to: https://sts10.github.io/2019/06/06/is-all-equal-function.html
fn is_all_equal(sku_codes: &Vec<Option<&String>>) -> bool {
    sku_codes.as_slice().windows(2).all(|w| w[0] == w[1])
}
