# Salesforce Exporter

Salesforce から複数のオブジェクトを取得し、CSV 形式に変換して Amazon S3 へアップロードするバッチです。設定ファイルで SOQL・増分抽出条件・S3 などを管理します。

## 必要環境

- Python 3.10 以降
- `pip install -r requirements.txt`

## 使い方

1. `config.yaml.example` をコピーして `config.yaml` を作成します。
2. S3・Salesforce の認証情報、出力先ディレクトリ、SOQL を設定します。
3. 実行します。

```bash
python main.py --config config.yaml
```

`--verbose` を付与するとデバッグログを出力します。

## 設定

設定ファイルは YAML 形式です。主要な項目は以下の通りです。

- `s3_info`
  - `bucket_name`、`access_key_id`、`secret_access_key` はアップロード先の S3 情報です。
  - `file_name` は S3 オブジェクトキーのプレフィックスです。CSV ファイル名が連結されます。
- `csv`
  - `output_directory` は CSV を一時的に保存するローカルディレクトリです。
  - `archive_directory` を指定するとアップロード成功後にファイルを移動します。
- `salesforce` は接続情報です。`domain` に `test` を指定すると Sandbox に接続します。`security_token` を空文字もしくは省略
  すると、IP 制限でトークン不要な環境としてログインします。
- `timezone` はファイル名や日付条件を計算する際のタイムゾーンです。
- `incremental`
  - `field` は増分取得の基準となる最終更新日などの列名です。
  - `where_template` は WHERE 句のテンプレートで、`{field}`、`{start_iso}`、`{end_iso}` などを利用できます。
  - `window_days` は取得期間の長さ、`end_offset_days` は「現在時刻から何日前まで」を表します。既定では「昨日の同時刻までの 24 時間分」を抽出します。
- `queries` 配列
  - `name` はクエリの識別子です。
  - `soql` は WHERE 句を除いた SOQL を記載します。テンプレートで生成した WHERE 句が自動的に付与されます。手動で `where` を指定するとその条件を使用します。
  - `output_file` を指定すると CSV ファイル名に利用されます。

## ファイル出力と S3 アップロード

各 SOQL の結果を CSV に出力し、`s3_info.file_name` のプレフィックスと組み合わせて S3 にアップロードします。アップロード成功後に `archive_directory` が設定されている場合はそのディレクトリへファイルを移動します。

## テスト

実際の Salesforce・S3 へは接続せず、設定ファイルの検証とコード整形のみを実施しています。
