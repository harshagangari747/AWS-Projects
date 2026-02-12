export async function fetchArticles(date, page = 1) {
  try {
    // Call the Netlify function
    const response = await fetch(
      `/.netlify/functions/fetchArticles?date=${date}&page=${page}`
    );

    if (!response.ok) {
      throw new Error("Failed to fetch articles");
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error("API Error:", error);
    throw error;
  }
}
