// netlify/functions/fetchArticles.js
// import fetch from "node-fetch"; // for Node 18+ may not be needed

export async function handler(event, context) {
  const { date, page } = event.queryStringParameters;

  try {
    // Example: calling your actual backend API (API Gateway)
    const response = await fetch(
      `${process.env.CLOUDFRONT_URL}?date=${date}&page=${page}`
    );
    const data = await response.json();

    return {
      statusCode: 200,
      body: JSON.stringify(data),
    };
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Failed to fetch articles" }),
    };
  }
}
