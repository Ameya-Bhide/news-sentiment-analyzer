import { useState } from "react";

export default function Filters({ onFilter }) {
  const [category, setCategory] = useState("");
  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");

  const handleApply = () => {
    onFilter({ category, start, end });
  };

  return (
    <div style={{ marginBottom: "1rem" }}>
      <label>
        Category: 
        <select value={category} onChange={(e) => setCategory(e.target.value)}>
          <option value="">All</option>
          <option value="business">Business</option>
          <option value="world">World</option>
          <option value="technology">Technology</option>
        </select>
      </label>

      <label style={{ marginLeft: "1rem" }}>
        Start Date:
        <input type="date" value={start} onChange={(e) => setStart(e.target.value)} />
      </label>

      <label style={{ marginLeft: "1rem" }}>
        End Date:
        <input type="date" value={end} onChange={(e) => setEnd(e.target.value)} />
      </label>

      <button style={{ marginLeft: "1rem" }} onClick={handleApply}>Apply</button>
    </div>
  );
}
