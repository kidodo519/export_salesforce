# Salesforce Exporter

Salesforce から複数のオブジェクトを取得し、CSV 形式に変換して Amazon S3 へアップロードするバッチです。設定ファイルで SOQL・増分抽出条件・S3 などを管理します。

## 変更点の概要

リポジトリに含まれる主なコンポーネントと役割は次の通りです。どのファイルが何を担っているかを把握しやすいように、構成を一覧化しました。

| ファイル / ディレクトリ | 役割 |
| --- | --- |
| `main.py` | 設定ファイルを読み込み、エクスポート処理を起動するエントリーポイントです。 |
| `salesforce_exporter/config.py` | YAML 設定をデータクラスに読み込み、増分条件の計算や検証を行います。 |
| `salesforce_exporter/exporter.py` | Salesforce から SOQL を実行し、CSV 出力・S3 アップロードをまとめて処理します。 |
| `salesforce_exporter/s3_uploader.py` | S3 へのアップロードや成功後のアーカイブ処理を担います。 |
| `config_facility.yaml` | 法人ごとの出力オン / オフと、参照する `config/[key].yaml` を管理する設定です。 |
| `config/` | 法人別の Salesforce・S3・SOQL 設定を `[key].yaml` として配置します。 |
| `config.yaml.example` | 法人別設定ファイルのサンプルです。 |
| `requirements.txt` | 必要な Python ライブラリをまとめています。 |


## 必要環境

- Python 3.10 以降
- `pip install -r requirements.txt`

## 使い方

1. `config_facility.yaml` で法人名・キー・出力オン / オフを管理します。
2. `config/[key].yaml` に、法人ごとの S3・Salesforce 認証情報、出力先ディレクトリ、SOQL を設定します。
3. 実行します。引数を省略すると `config_facility.yaml` を読み込み、有効な法人を順番に処理します。

```bash
python main.py
```

特定の法人設定だけを単体実行する場合は、法人別 YAML を直接指定できます。

```bash
python main.py --config config/arayatotoan.yaml
```

`--verbose` を付与するとデバッグログを出力します。

pyinstaller用

```bash
pyinstaller .\main.py --add-data "config_facility.yaml;." --add-data "config;config"
```

## 設定

### 複数法人設定

`config_facility.yaml` は法人別設定ファイルをまとめるスイッチボードです。`key` は `config_directory` 配下の `[key].yaml` と対応します。`output` で法人ごとの処理対象、`upload_to_s3` で S3 アップロード、`local_csv_output` でローカル CSV 保存をそれぞれ YAML の真偽値（`true` / `false`）で切り替えます。

```yaml
config_directory: config
facilities:
  - name: あらや滔々庵
    key: arayatotoan
    output: true
    upload_to_s3: true
    local_csv_output: true
  - name: 季さら
    key: kisara
    output: true
    upload_to_s3: true
    local_csv_output: true
```

上記の場合、`config/arayatotoan.yaml` と `config/kisara.yaml` が参照されます。`output: false` にした法人は実行時にスキップされます。`upload_to_s3: false` の法人は CSV 生成後の S3 アップロードを行いません。`local_csv_output: false` の法人はローカル CSV を保存せず、`upload_to_s3: true` の場合はメモリ上で生成した CSV を直接 S3 にアップロードします。

法人別設定ファイルは YAML 形式です。主要な項目は以下の通りです。

- `s3_info`
  - `bucket_name`、`access_key_id`、`secret_access_key` はアップロード先の S3 情報です。
  - `file_name` は S3 オブジェクトキーのプレフィックスです。CSV ファイル名が連結されます。
- `csv`
  - `output_directory` は `local_csv_output: true` のときに CSV を保存するローカルディレクトリです。
  - `archive_directory` を指定すると、`local_csv_output: true` かつ `upload_to_s3: true` でアップロード成功後にファイルを移動します。
  - `encoding` を指定すると CSV の文字コードを変更できます。既定値は `utf-8` で、`shift_jis` を指定すると SJIS で書き出します。
- `output_control`
  - 法人別 YAML を `--config` で直接実行する場合に、`upload_to_s3` と `local_csv_output` の既定値を設定できます。`config_facility.yaml` 経由で実行する場合は、法人側の `upload_to_s3` / `local_csv_output` が優先されます。
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
  - `write_output` を `false` にすると SOQL は実行しますが CSV を生成せず、後続クエリや結合用に結果のみをキャッシュします。
  - `incremental` をクエリ単位で指定すると、増分取得の設定を上書きまたは無効化できます。`false` を指定すると常に全件出力、マップ形式で `field` や `window_days` を設定するとその値を使用します。
  - `relationship_filters` を指定すると、先に実行したクエリの結果から ID を収集して `IN` 条件を自動生成できます。`source_query`（参照元クエリ名）、`source_field`（参照元の列名）、`target_field`（対象クエリでフィルタする列名）を設定すると、取得した ID を `target_field IN (...)` 形式で追加します。ID が多い場合に備えて `chunk_size`（既定値 200）で分割し、複数回に分けて SOQL を実行します。

- `combined_outputs` 配列
  - `name` は結合結果の識別子、`base_query` は結合の起点となるクエリ名です。
  - `joins` で複数の結合定義を並べると、順番に `pandas.merge` を実行して列を取り込みます。`left_on`／`right_on` で結合キー（単一または配列）を指定し、`suffixes` で重複カラム名に付くサフィックスを制御できます（省略時は `("", "_<source_query>")`）。
  - `joins[].aggregate` を指定すると、結合前に参照先データを `group_by` で集約できます。`fields` に対象カラム、`function` に `sum` / `min` / `max` / `mean` / `first` / `last` / `count`、未結合時に補完したい値がある場合は `fill_value` を指定します。集計対象ではないが同じグループから後続 JOIN 用のキーなどを残したい列は `carry_fields` に指定します。1 対多の参照先を集約してから結合することで、起点データの行数を維持できます。
  - `output_file` を指定すると生成される CSV のファイル名になります。省略時は `name` が使用されます。
  - サンプル設定では `Reservations_*` と `Sales_*` の元データに関連オブジェクト（`Contact`、`Plan`、`AccountAcount`、`AccountMaster`）
    を順番に結合し、最終的な CSV を 7 ファイルにまとめています。


## ファイル出力と S3 アップロード

各 SOQL または `combined_outputs` の結果は、法人ごとの `local_csv_output` / `upload_to_s3` に従って処理されます。`local_csv_output: true` の場合は `output_directory` に CSV を保存します。`upload_to_s3: true` の場合は `s3_info.file_name` のプレフィックスと組み合わせて S3 にアップロードします。両方が `true` でアップロードに成功し、`archive_directory` が設定されている場合は、そのディレクトリへファイルを移動します。`local_csv_output: false` かつ `upload_to_s3: true` の場合は、ローカルに CSV を保存せずにメモリ上で生成した CSV を S3 にアップロードします。

## テスト

実際の Salesforce・S3 へは接続せず、設定ファイルの検証とコード整形のみを実施しています。
