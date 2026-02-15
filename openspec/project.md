# Project Context

## Purpose
**twstock** is a Python library for fetching and analyzing Taiwan Stock Market data. The project provides:
- Real-time and historical stock data from Taiwan Stock Exchange (TWSE) and Taipei Exchange (TPEX)
- Technical analysis with indicators (moving averages, bias ratios, pivot points, Best Four Point signals)
- Chip concentration analysis tracking large holders and retail investors
- Automated signal generation with historical backtesting (expected returns, win rates)
- Daily Excel reports with comprehensive technical and fundamental analysis

## Tech Stack

### Core Language & Runtime
- **Python 3.12+**: Primary language
- **Poetry**: Dependency management and virtual environment

### Data & Analysis
- **pandas 2.2.1+**: Data manipulation and analysis
- **scipy**: Statistical computations
- **ta-lib 0.4.28+**: Technical analysis indicators
- **matplotlib 3.8.3+**: Chart visualization
- **mplfinance**: Financial chart plotting

### Database
- **PostgreSQL**: Primary data storage
- **SQLAlchemy 2.0.23+**: ORM with async support
- **asyncpg 0.29.0+**: PostgreSQL async driver
- **psycopg2-binary**: PostgreSQL sync driver (fallback)

### HTTP & Web Scraping
- **httpx 0.27.0+** (with HTTP/2): Async HTTP client for API requests
- **playwright 1.56.0+**: Browser automation for header initialization
- **requests 2.31.0+**: Legacy HTTP support
- **beautifulsoup4 4.12.3+**: HTML parsing
- **lxml 5.1.0+**: XML/HTML processing

### Data Export
- **openpyxl 3.1.2+**: Excel file generation with formatting
- **xlsxwriter 3.2.0+**: Alternative Excel writer
- **feather-format**: Fast DataFrame serialization

### Configuration & Validation
- **pydantic 2.5.0+**: Data validation and settings
- **python-dotenv**: Environment variable management

### Development Tools
- **ipykernel**: Jupyter notebook support

## Project Conventions

### Documentation Language Policy

**文件語言優先順序：繁體中文**

This project uses Traditional Chinese (繁體中文) as the primary language for all documentation, including:
- OpenSpec change proposals (`openspec/changes/*/proposal.md`, `design.md`, `tasks.md`)
- OpenSpec specification files (`openspec/changes/*/specs/*/spec.md`)
- Project README and user-facing documentation
- Code comments explaining business logic (Taiwan stock market specific)

English is acceptable for:
- Technical terminology (framework names, programming concepts)
- Code identifiers (variables, functions, classes)
- Configuration keys and environment variables
- External library/API references

Examples of good hybrid usage:
- ✓ "使用 FastAPI 建立 RESTful API，處理籌碼集中度資料查詢"
- ✓ "Stock 類別繼承 Analytics，提供技術分析功能"
- ✗ "Use FastAPI to build RESTful API for chip concentration data queries" (too much English)
- ✗ "股票類別繼承分析類，提供科技分析功能" (unnecessary translation of technical terms)

### Code Style
- **Async/Await Patterns**: Extensive use of async/await for concurrent operations
- **Class-Based Architecture**: Core functionality organized into classes (Stock, All, Analytics, WantgooFetcher, DatabaseManager)
- **Type Hints**: Use type hints where appropriate for clarity
- **Naming Conventions**:
  - Classes: PascalCase (e.g., `WantgooFetcher`, `DatabaseManager`)
  - Functions/Methods: snake_case (e.g., `fetch_stock_info`, `get_concentration_data`)
  - Constants: UPPER_SNAKE_CASE (e.g., `MAX_API_WORKERS`, `DATABASE_URL`)
- **Docstrings**: Document complex methods and classes
- **Line Length**: Prefer readability over strict limits

### Architecture Patterns

#### Core Components
1. **Stock** (`twstock/stock.py`): Main class for individual stock operations
   - Inherits from Analytics for technical analysis
   - Uses WantgooFetcher for data retrieval
   - Stores data in PostgreSQL

2. **All** (`twstock/all.py`): Batch processor for multiple stocks
   - Handles concurrent fetching with asyncio
   - Batch loading from PostgreSQL (avoids N+1 queries)
   - Generates comprehensive Excel reports

3. **Analytics** (`twstock/analytics.py`): Base class for technical analysis
   - Moving averages, bias ratios, pivot points
   - Best Four Point analysis for buy/sell signals

4. **WantgooFetcher** (`twstock/wantgoo.py`): HTTP client abstraction
   - Async requests with httpx + HTTP/2
   - Auto-initializes headers with Playwright
   - Smart caching with API date detection

5. **DatabaseManager** (`twstock/database.py`): PostgreSQL abstraction
   - SQLAlchemy ORM with async support
   - Batch operations for performance
   - Smart update tracking

#### Design Principles
- **Smart Caching**: Only update data when API has newer information
- **Batch Operations**: Single SQL queries for multiple stocks
- **Async by Default**: Concurrent operations for performance
- **Rate Limiting**: Semaphores to respect API limits
- **Fail Gracefully**: Continue processing on individual stock failures

### Testing Strategy
- **Framework**: Python unittest
- **Test Discovery**: `python3 -m unittest discover -s test`
- **Test Organization**: Tests in `test/` directory
- **Coverage**: Focus on core functionality (Stock, Analytics, fetchers)

### Git Workflow
- **Main Branch**: `master`
- **Feature Branches**: Create feature branches for development (e.g., `trend`, feature names)
- **Commit Style**: Conventional commits preferred (feat:, fix:, refactor:, docs:)
- **Before Merge**: Ensure tests pass and core functionality works

## Domain Context

### Taiwan Stock Market Specifics
- **TWSE**: Taiwan Stock Exchange (major stocks)
- **TPEX**: Taipei Exchange (OTC stocks)
- **Trading Days**: Mon-Fri (excluding Taiwan holidays)
- **Data Availability**: T+1 (previous trading day data available next day)
- **Stock IDs**: 4-6 digit numeric codes (e.g., 2330 for TSMC)

### Technical Analysis
- **Best Four Point**: Entry/exit signals based on price and moving averages
- **Moving Averages**: 5, 10, 20, 60, 240-day periods commonly used
- **Bias Ratio**: Percentage deviation from moving average
- **Pivot Points**: Support/resistance levels calculated from high/low/close

### Chip Concentration Analysis
- **Large Holders**: moreThan400 (>400 shares), moreThan1000 (>1000 shares)
- **Retail Investors**: lessThan20 (<20 shares)
- **Weekly Data**: Concentration data updated weekly
- **10 Signals**: Automated detection of accumulation/distribution patterns
- **Signal Scoring**: 0-100 scale based on multiple signal triggers
- **Backtesting**: Historical expected return and win rate for each signal

### Performance Metrics
- **First Run**: ~10 minutes (API fetching for 1000+ stocks)
- **Subsequent Runs**: ~2 minutes (cache hit for most stocks)
- **Concurrency**: 10-50 workers depending on operation type
- **Throughput**: ~24 stocks/sec for analysis, ~20 stocks/sec for chip concentration

## Important Constraints

### API Rate Limits
- **Wantgoo API**: Respect rate limits with semaphore controls
- **Concurrency Tuning**: Use environment variables to adjust worker count
  - `MAX_API_WORKERS`: Default 10 (API updates)
  - `MAX_WORKERS`: Default 20 (analysis)
  - `MAX_CONCENTRATION_WORKERS`: Default 10 (chip concentration)

### Data Availability
- **Real-time Lag**: Data typically available next trading day (T+1)
- **Weekends/Holidays**: No new data on non-trading days
- **Historical Limits**: API may have limited historical data depth

### Database Requirements
- **PostgreSQL 12+**: Required for async operations
- **Connection Pooling**: Managed by SQLAlchemy
- **Storage**: ~1GB for full historical data (2700+ stocks)

### Performance Considerations
- **Memory Usage**: High with MAX_WORKERS=50 (reduce to 20-30 if needed)
- **First Run**: Long initialization time for full data fetch
- **Browser Automation**: Playwright requires system dependencies

## External Dependencies

### Primary Data Source
- **Wantgoo API** (www.wantgoo.com):
  - Stock basic information (name, industry, outstanding shares)
  - Daily OHLCV data (open, high, low, close, volume)
  - Chip concentration data (large holders, retail investors)
  - Institutional investor holdings
  - Margin trading data

### Database
- **PostgreSQL**:
  - Default: `postgresql+asyncpg://twstock_user:twstock_password123@localhost:5432/twstock`
  - Configurable via `DATABASE_URL` environment variable
  - Docker Compose setup available for local development

### Browser Automation
- **Playwright**: Used for Wantgoo header initialization
  - Falls back to basic cookies if unavailable
  - Requires chromium browser installation

### Optional Services
- **Jupyter**: For interactive development and testing
- **Docker**: For PostgreSQL containerization
