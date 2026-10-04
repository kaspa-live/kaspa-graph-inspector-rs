import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

const root = document.getElementById("root");

if (root === null) {
  throw new Error("missing application root");
}

createRoot(root).render(<StrictMode />);
