# 📊 News Sentiment Analyzer

A Python-based pipeline for scraping financial, business, world, and technology news from multiple sources, analyzing sentiment with **VADER** and **FinBERT**, and exploring potential links to market movements.  

---

## 🚀 Features
- **Multi-source scraping**: Pulls headlines from Reuters, BBC, CNN, The Guardian, NPR, NYTimes, MarketWatch, TechCrunch, and more (via RSS feeds).  
- **Sentiment analysis**:  
  - **VADER**: General-purpose sentiment scoring (-1 to +1).  
  - **FinBERT**: Financial domain-specific sentiment classifier (Positive / Negative / Neutral).  
- **Daily summaries**: Aggregates headline sentiment per day and category (business, world, technology).  
- **Visualizations**:  
  - Overall daily sentiment trends.  
  - Category-specific trends (business, world, technology).  
  - Stored under `sentiment_plots/`.  
- **Market analysis (experimental)**: Correlates sentiment with major indexes (**S&P 500, NASDAQ, Dow Jones**) and explores lead-lag effects.  

## ⚙️ Setup

### 1. Clone and enter project
```bash
git clone https://github.com/Ameya-Bhide/news-sentiment-analyzer.git
cd news-sentiment-analyzer