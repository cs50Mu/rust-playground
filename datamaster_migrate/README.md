

## 遇到的问题

1. sqlx 不能检查 outer join 的字段可能null问题，看github上说是解决了，但我试了下并没有检查出来

参考：[patch nullable inference in Postgres using EXPLAIN](https://github.com/launchbadge/sqlx/pull/566)

但根据这个[回答](https://github.com/launchbadge/sqlx/issues/367#issuecomment-799829096)，需要在可能为 null 字段的结尾上加上`?`，这样就可以了：

```sql
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
```

2. 在跑脚本的时候遇到这个错误（数据库是pg）：canceling statement due to conflict with recovery

查了下，貌似是因为在从库查询期间，若主库有写入就会报这个错，要解决的话需要修改数据库配置，但DBA不一定同意。。所以我们通过减少 limit 数量来缩短一次查询的时间来缓解这个问题（或者尽量避开频繁修改的时间）

参考：

- [Deal with Postgresql Error -canceling statement due to conflict with recovery- in psycopg2](https://stackoverflow.com/questions/38514312/deal-with-postgresql-error-canceling-statement-due-to-conflict-with-recovery-i)
- [PostgreSQL ERROR: canceling statement due to conflict with recovery](https://stackoverflow.com/questions/14592436/postgresql-error-canceling-statement-due-to-conflict-with-recovery)

3. 如何将 `Option` 转成 `Result`？

可以使用 `ok_or` 和 `ok_or_else`

参考：[converting an Option into a Result](https://stackoverflow.com/questions/37890405/is-there-a-way-to-simplify-converting-an-option-into-a-result-without-a-macro)

4. rust 中如何表示 decimal？

参考：[Floating-point cheat sheet for Rust](https://floating-point-gui.de/languages/rust/)

5. mongoDB 中如何查找缺失字段的记录?

`db.product.find({"skus.code": {$exists: false}})`

参考：[Query for Null or Missing Fields](https://docs.mongodb.com/manual/tutorial/query-for-null-fields/)

为啥会有这个需求呢？因为是遇到了一个问题：往mongoDB里插入数据时，报错：`E11000 duplicate key error collection: datamaster.product index: skus.code_1 dup key: { : null }`，重复的 key 是 null？这是什么鬼。。一开始还以为是该字段会有null的情况呢，可是排查了一下并没有。。后来搜索到这个 stackoverflow [问题](https://dba.stackexchange.com/questions/211522/mongodb-show-duplicate-key-is-null)，原来不存在的字段会被当成 null 插进索引里，而这又是个唯一索引，只能存在一个 null 值

参考：[MongoDB show duplicate key is null?](https://dba.stackexchange.com/questions/211522/mongodb-show-duplicate-key-is-null)

6. 发现一个传递文件的好方法

之前一直用 scp，不过这次发现它没法排除文件，于是搜到了 rsync：

`rsync -av -e ssh --exclude='target' --exclude='.git' --exclude='.env' datamaster_migrate muchunyu@192.168.152.134:/home/muchunyu/linuxfish/datamaster`

参考：[How to Use rsync to Exclude Files and Directories in Data Transfer](https://phoenixnap.com/kb/rsync-exclude-files-and-directories)
