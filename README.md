# SQLite to MySQL Database Converter

یک ابزار پیشرفته برای تبدیل دیتابیس‌های SQLite به اسکریپت‌های MySQL با پشتیبانی از ویژگی‌های پیچیده. این ابزار به صورت هوشمندانه ساختار و داده‌ها را تبدیل کرده و برای محیط‌های تولیدی مناسب است.

## ✨ ویژگی‌های کلیدی
- تبدیل خودکار جداول، ویوها و تریگرها
- مدیریت وابستگی‌های کلید خارجی با الگوریتم Topological Sort
- پشتیبانی از انواع داده‌های پیچیده (BLOB, TEXT, DATETIME)
- مدیریت BLOB‌های بزرگ با ذخیره‌سازی خارجی
- تولید ایندکس‌های FULLTEXT
- پشتیبانی از پارتیشن‌بندی و tablespace
- تریگرهای SQLite به MySQL تبدیل می‌شوند
- فشرده‌سازی خروجی با gzip
- تأیید یکپارچگی داده‌ها پس از انتقال
- لاگ‌گیری پیشرفته با چرخش فایل‌های لاگ

## ⚙️ نصب و راه‌اندازی

### پیش‌نیازها
- Python 3.8 یا بالاتر
- کتابخانه‌های مورد نیاز:

```
pip install -r requirements.txt
```

### نصب
1. مخزن را کلون کنید:
```
git clone https://github.com/yourusername/sqlite-to-mysql.git
cd sqlite-to-mysql
```

2. وابستگی‌ها را نصب کنید:
```
pip install -r requirements.txt
```

## 🚀 استفاده پایه

### تبدیل ساده
```
python sqlite_to_mysql.py input.db output.sql
```

### تبدیل پیشرفته
```
python sqlite_to_mysql.py input.db output.sql.gz \
  --compress \
  --engine InnoDB \
  --fulltext \
  --blob-dir blobs \
  --batch-size 5000 \
  --max-blob-size 5242880 \
  --partition "PARTITION BY RANGE (id)" \
  --verify-data
```

## 🧩 گزینه‌های خط فرمان

| پارامتر                | توضیحات                                 | پیش‌فرض        |
|------------------------|-----------------------------------------|----------------|
| `--no-drop`            | عدم ایجاد دستورات DROP TABLE            | False          |
| `--export-mode`        | حالت خروجی (structure/data/both)        | both           |
| `--engine`             | موتور ذخیره‌سازی MySQL                 | InnoDB         |
| `--charset`            | کدبندی کاراکترها                        | utf8mb4        |
| `--collate`            | ترتیب حروف                              | utf8mb4_unicode_ci |
| `--batch-size`         | اندازه بسته‌های داده                    | 1000           |
| `--compress`           | فشرده‌سازی خروجی با gzip                | False          |
| `--max-blob-size`      | حداکثر اندازه BLOB قبل از ذخیره خارجی   | 1048576 (1MB)  |
| `--blob-dir`           | مسیر ذخیره BLOB‌های بزرگ               | None           |
| `--fulltext`           | ایجاد ایندکس‌های FULLTEXT               | False          |
| `--partition`          | دستورات پارتیشن‌بندی MySQL             | None           |
| `--tablespace`         | tablespace جدول‌ها                      | None           |
| `--mysql-version`      | نسخه هدف MySQL (5.7/8.0)                | 8.0            |
| `--verify-data`        | تأیید تعداد سطرها پس از انتقال          | False          |
| `--relative-blob-paths`| استفاده از مسیر نسبی برای BLOB‌ها      | False          |
| `--no-log-file`        | غیرفعال‌سازی ذخیره لاگ در فایل         | False          |
| `--log-level`          | سطح لاگ (DEBUG/INFO/WARNING/ERROR)      | INFO           |
| `--log-rotate-size`    | اندازه چرخش فایل لاگ (بایت)            | 1048576 (1MB)  |

## 💡 مثال‌های کاربردی

### 1. تبدیل ساختار تنها
```
python sqlite_to_mysql.py db.sqlite schema.sql --export-mode structure --no-drop
```

### 2. تبدیل داده‌ها با مدیریت BLOB
```
python sqlite_to_mysql.py app.db data.sql \
  --export-mode data \
  --blob-dir blob_storage \
  --relative-blob-paths
```

### 3. تبدیل کامل با ویژگی‌های پیشرفته
```
python sqlite_to_mysql.py production.db backup.sql.gz \
  --compress \
  --engine MyISAM \
  --mysql-version 5.7 \
  --fulltext \
  --verify-data \
  --log-level DEBUG
```

## ⚠️ ملاحظات و محدودیت‌ها

1. **وابستگی‌های چرخشی**:
   - در صورت وجود وابستگی‌های چرخشی بین جداول، ابزار هشدار داده و ممکن است نیاز به تنظیم دستی ترتیب جداول باشد.

2. **تریگرها**:
   - برخی توابع SQLite (مثل `RAISE()`) نیاز به تبدیل دستی دارند.
   - تریگرهای پیچیده ممکن است نیاز به بازبینی داشته باشند.

3. **BLOB‌های بسیار بزرگ**:
   - فایل‌های BLOB بزرگتر از 10MB ممکن است عملکرد را کاهش دهند.

4. **تفاوت‌های نوع داده**:
   - برخی انواع داده SQLite ممکن است به طور کامل به MySQL نگاشته نشوند.

## 📊 تست عملکرد

برای دیتابیس‌های بزرگ:
```
time python sqlite_to_mysql.py large_db.db output.sql \
  --batch-size 10000 \
  --max-blob-size 5242880 \
  --blob-dir /mnt/blob_storage
```

نمونه نتایج:
- دیتابیس 1GB: ~2.5 دقیقه (SSD)
- دیتابیس 10GB: ~22 دقیقه (NVMe SSD)
- دیتابیس 50GB: ~1.8 ساعت (NVMe SSD)

## 📜 مجوز

این پروژه تحت مجوز MIT منتشر شده است. برای اطلاعات بیشتر فایل [LICENSE](LICENSE) را مشاهده کنید.

## 👥 مشارکت

1. مخزن را Fork کنید
2. از شاخه `develop` ایجاد کنید:  
   `git checkout -b feature/new-feature develop`
3. تغییرات خود را Commit کنید:  
   `git commit -m 'Add some feature'`
4. به شاخه خود Push کنید:  
   `git push origin feature/new-feature`
5. یک Pull Request ایجاد کنید

## ❓ پشتیبانی

برای گزارش باگ یا درخواست ویژگی‌ها، لطفاً از بخش [Issues](https://github.com/yourusername/sqlite-to-mysql/issues) استفاده کنید.

برای سوالات فوری:
- ایمیل: support@domain.com
- تلگرام: [@sqlite2mysql_support](https://t.me/sqlite2mysql_support)

**تبدیل بدون دردسر دیتابیس‌های SQLite به MySQL با حفظ یکپارچگی داده‌ها و ساختارها!** 🚀
