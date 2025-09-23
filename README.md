# 📈 News Sentiment Dashboard

A web app that tracks news sentiment across multiple categories (Business, World, Technology) and compares it to major stock indexes (S&P 500, NASDAQ, Dow Jones).  

🔗 **Live Dashboard:** [View on Vercel](https://sentivest.vercel.app/)  

---

## ✨ Features
- Aggregates news headlines every 6 hours with a Scrapy spider  
- Analyzes sentiment with **VADER** and **FinBERT**  
- Stores processed data in **Supabase** (CSV-based)  
- Interactive charts built with **Next.js + Recharts + Tailwind CSS**  
- Compare sentiment trends against market returns  

---

## 🛠️ Tech Stack
- **Frontend:** Next.js, Tailwind CSS, Recharts  
- **Backend / APIs:** Next.js API routes, Supabase storage  
- **Data Pipeline:** Scrapy, Python (VADER, FinBERT), GitHub Actions  

---

## 📂 Repository
This repo contains both the data pipeline and the dashboard frontend:  
- `news_sentiment/` → Python Scrapy spider + sentiment analysis  
- `frontend/` → Next.js dashboard  
- 'backend/' → Express backend, now redundant with Vercel deployment structure
---

## 🚀 Deployment
- **Frontend:** Vercel (Next.js)  
- **Data Storage:** Supabase Public Bucket  
- **Automation:** GitHub Actions (runs scraper every 6 hours)  

---
