import express from "express";
import cors from "cors";
import sentimentRoutes from "./routes/sentiment.js";
import marketRoutes from "./routes/market.js";

const app = express();
const PORT = process.env.PORT || 4000;

app.use(cors());
app.use(express.json());

// Routes
app.use("/api/sentiment", sentimentRoutes);
app.use("/api/market", marketRoutes);

app.listen(PORT, () => {
  console.log(`🚀 Backend running on http://localhost:${PORT}`);
});
