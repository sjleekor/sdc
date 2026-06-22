# Local PostgreSQL Migration Runbook

작성 기준: 2026-06-18 KST

이 문서는 로컬 Docker PostgreSQL을 삭제하고, Mac에 설치한 PostgreSQL 18로 `sdc` 로컬 DB를 다시 구성하기 위한 실행 순서다. 데이터 자체는 나중에 `sj2-server`에서 다시 동기화한다는 전제이므로 Docker DB volume 백업은 기본 절차에서 제외한다.

## 현재 확인된 상태

- 실행 중인 로컬 PostgreSQL 컨테이너: `my-postgres`
- 컨테이너 이미지: `postgres:17`
- 포트: host `5432` -> container `5432`
- Docker 데이터 volume: `postgres_pgdata`
- 추가 bind mount: `/Users/whishaw/docker/postgres/conf/postgresql.conf` -> `/etc/postgresql/postgresql.conf`
- 현재 `.env`의 DB 대상: `postgresql://myuser:<password>@localhost:5432/mydb`

주의: repository의 `docker-compose.yml` 기본 DB 서비스는 `sdc-postgres` / `postgres:16` / `krx_user` / `krx_data` 기준이지만, 현재 실제로 떠 있는 로컬 DB는 별도 `my-postgres` 컨테이너다. 따라서 삭제 작업은 compose 기준이 아니라 `my-postgres`와 `postgres_pgdata` 기준으로 진행한다.

## 설치 방식 결정

권장 설치 방식은 Homebrew의 `postgresql@18`이다.

- Homebrew: CLI 중심 개발 흐름에 가장 잘 맞고, `brew services`로 백그라운드 서비스 관리가 단순하다.
- Postgres.app: GUI와 메뉴바 앱이 편하지만, 현재 목적은 장기 실행 로컬 DB와 CLI 기반 파이프라인 실행이므로 우선순위가 낮다.
- EDB installer: pgAdmin/StackBuilder까지 포함하는 완성형 installer지만, 디스크 여유가 적은 상황에서는 상대적으로 무겁다.
- MacPorts/Fink: 이미 해당 패키지 매니저를 쓰는 경우가 아니면 새로 도입할 이유가 약하다.

2026-06-18 현재 Homebrew `postgresql@18`은 PostgreSQL 18 계열 최신 minor를 제공한다. 특정 patch 버전을 고정할 필요가 없다면 `brew install postgresql@18`을 사용한다.

## Phase 0. 실행 전 확인

목표: 삭제 대상이 `my-postgres`와 `postgres_pgdata`가 맞는지 확인하고, `localhost:5432`가 Docker 컨테이너에 의해 점유 중인지 확인한다.

```bash
cd /Users/whishaw/wss_p/stock_data_collector

python3 -c "from pathlib import Path; from urllib.parse import urlparse; import re; text=Path('.env').read_text(); m=re.search(r'^DB_DSN=(.*)$', text, re.M); d=(m.group(1).strip().strip('\"\\'') if m else ''); u=urlparse(d); print('user='+(u.username or '')); print('password=<set>' if u.password else 'password=<empty>'); print('host='+(u.hostname or '')); print('port='+(str(u.port) if u.port else '')); print('database='+(u.path.lstrip('/') if u.path else ''))"

docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Ports}}\t{{.Status}}'
docker inspect my-postgres --format '{{json .Mounts}}'
lsof -nP -iTCP:5432 -sTCP:LISTEN || true
df -h ~
```

기대값:

- `.env` 파싱 결과가 `user=myuser`, `host=localhost`, `port=5432`, `database=mydb`로 나온다.
- `docker ps`에 `my-postgres` / `postgres:17` / `0.0.0.0:5432->5432/tcp`가 보인다.
- `docker inspect`의 mount에 `Name":"postgres_pgdata"`가 보인다.
- `lsof`에서 Docker가 `5432`를 listen 중이다.

기대값과 다르면 여기서 중단하고 삭제 대상을 다시 확인한다.

## Phase 1. Docker DB 삭제

목표: 현재 Docker PostgreSQL 프로세스와 데이터 volume을 제거해 디스크 공간을 회수하고, `localhost:5432`를 비운다.

```bash
docker stop my-postgres
docker rm my-postgres
docker volume rm postgres_pgdata
```

삭제 확인:

```bash
docker ps -a --filter name=my-postgres
docker volume ls --filter name=postgres_pgdata
lsof -nP -iTCP:5432 -sTCP:LISTEN || true
docker system df
```

기대값:

- `my-postgres` 컨테이너가 더 이상 보이지 않는다.
- `postgres_pgdata` volume이 더 이상 보이지 않는다.
- `5432` listen 프로세스가 없다.

선택 정리:

```bash
docker image rm postgres:17
```

`docker system prune -f --volumes`는 다른 프로젝트의 미사용 image/volume까지 삭제할 수 있으므로 이 절차의 기본 명령에 포함하지 않는다.

`/Users/whishaw/docker/postgres/conf/postgresql.conf`는 작은 설정 파일이다. 공간 회수 효과는 작으므로 즉시 삭제하지 말고, 더 이상 Docker PostgreSQL을 재사용하지 않는다고 확정한 뒤 별도로 정리한다.

## Phase 2. PostgreSQL 18 설치 및 시작

목표: Mac host에 PostgreSQL 18을 설치하고 `localhost:5432`에서 실행한다.

```bash
brew update
brew install postgresql@18
```

`psql`, `createdb`, `createuser` 등을 현재 shell에서 바로 쓰기 위해 PATH를 설정한다.

```bash
grep -q 'postgresql@18' ~/.zshrc || echo 'export PATH="$(brew --prefix postgresql@18)/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

서비스 시작:

```bash
brew services start postgresql@18

postgres --version
pg_isready -h localhost -p 5432
lsof -nP -iTCP:5432 -sTCP:LISTEN || true
```

기대값:

- `postgres --version`이 `PostgreSQL 18.x`를 출력한다.
- `pg_isready`가 `accepting connections`를 출력한다.
- `5432` listen 프로세스가 Docker가 아니라 PostgreSQL/Homebrew 서비스다.

## Phase 3. `.env`와 같은 role/database 생성

목표: `.env`의 `DB_DSN`을 그대로 사용하기 위해 `myuser` role과 `mydb` database를 만든다.

새 role 생성 시 `.env`의 `DB_DSN`에 들어 있는 비밀번호를 입력한다. URL encoding이 들어간 비밀번호라면 PostgreSQL role에는 decoding된 실제 비밀번호를 설정한다. 예를 들어 DSN에 `%21`이 있으면 실제 비밀번호 문자는 `!`이다.

```bash
createuser --maintenance-db=postgres --login --pwprompt myuser
createdb --maintenance-db=postgres --owner=myuser mydb
```

이미 role 또는 database가 있다는 오류가 나면 아래처럼 상태를 확인한다.

```bash
psql -d postgres -c '\du'
psql -d postgres -c '\l'
```

role은 있는데 비밀번호를 다시 맞춰야 하면 interactive `psql`에서 처리한다.

```bash
psql -d postgres
```

`psql` 안에서:

```sql
ALTER ROLE myuser WITH LOGIN;
\password myuser
\q
```

database가 이미 있는데 owner가 다르면:

```bash
psql -d postgres -c 'ALTER DATABASE mydb OWNER TO myuser;'
```

접속 검증:

```bash
read -s PGPASSWORD
export PGPASSWORD

psql -h localhost -p 5432 -U myuser -d mydb \
  -c 'select version(), current_database(), current_user;'

unset PGPASSWORD
```

기대값:

- `current_database`가 `mydb`다.
- `current_user`가 `myuser`다.
- `version()`이 PostgreSQL 18.x를 포함한다.

## Phase 4. SDC 스키마 생성

목표: repository의 DDL 원본인 `sql/postgres_ddl.sql`을 새 로컬 DB에 적용한다.

권장 경로는 프로젝트 CLI를 사용하는 것이다. CLI가 `.env`의 `DB_DSN`을 읽고 `PostgresStorage.init_schema()`에서 `sql/postgres_ddl.sql`을 실행한다.

```bash
cd /Users/whishaw/wss_p/stock_data_collector

uv sync
uv run krx-collector db init
```

스키마 검증:

```bash
read -s PGPASSWORD
export PGPASSWORD

psql -h localhost -p 5432 -U myuser -d mydb \
  -c "select count(*) as public_base_tables from information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE';"

psql -h localhost -p 5432 -U myuser -d mydb -c '\dt public.*'

unset PGPASSWORD
```

기대값:

- `public_base_tables`가 `23`이다.
- 주요 테이블 예: `stock_master`, `daily_ohlcv`, `dart_financial_statement_raw`, `stock_metric_fact`, `krx_security_flow_raw`, `common_feature_daily_fact`.

## Phase 5. 나중에 sj2-server에서 데이터 동기화

목표: 빈 로컬 스키마에 운영 DB 데이터를 다시 복제한다.

첫 동기화 또는 schema drift까지 교정하려는 경우:

```bash
cd /Users/whishaw/wss_p/stock_data_collector

uv run krx-collector db sync-remote \
  --ssh-host whi@sj2-server \
  --full-refresh \
  --all-tables
```

참고:

- `--all-tables`는 반드시 `--full-refresh`와 함께 사용한다.
- 이 경로는 관리 대상 mirror 테이블을 drop/recreate 후 원격 데이터를 binary COPY로 복제한다.
- `sync_checkpoints`, 로컬 `ingestion_runs` 같은 운영 보조 테이블은 전체 mirror 대상과 성격이 다를 수 있다.

완료 후 빠른 확인:

```bash
read -s PGPASSWORD
export PGPASSWORD

psql -h localhost -p 5432 -U myuser -d mydb \
  -c "select 'stock_master' as table_name, count(*) from stock_master union all select 'daily_ohlcv', count(*) from daily_ohlcv union all select 'stock_metric_fact', count(*) from stock_metric_fact;"

unset PGPASSWORD
```

## 중단/복구 기준

- Phase 0에서 `my-postgres` 또는 `postgres_pgdata`가 보이지 않으면 삭제 명령을 실행하지 않는다.
- Phase 1 후 `5432`가 여전히 Docker에 의해 점유되어 있으면 PostgreSQL 18 설치를 진행하기 전에 점유 프로세스를 먼저 확인한다.
- Phase 2에서 `pg_isready`가 실패하면 role/database 생성으로 넘어가지 않는다.
- Phase 3에서 `myuser` 접속 검증이 실패하면 `db init`을 실행하지 않는다.
- Phase 4에서 `db init`이 실패하면 같은 DB에 부분 생성 테이블이 남아 있을 수 있다. 비어 있는 새 DB를 원하면 `dropdb mydb` 후 `createdb --owner=myuser mydb`부터 다시 진행한다.

## 되돌리기

Docker volume을 삭제한 뒤에는 기존 로컬 DB 데이터는 복구할 수 없다. 이 runbook의 복구 경로는 `sj2-server`에서 다시 동기화하는 것이다.

Homebrew PostgreSQL 서비스를 중지하려면:

```bash
brew services stop postgresql@18
```

새 로컬 DB를 완전히 다시 만들려면:

```bash
dropdb --maintenance-db=postgres mydb
createdb --maintenance-db=postgres --owner=myuser mydb
uv run krx-collector db init
```
