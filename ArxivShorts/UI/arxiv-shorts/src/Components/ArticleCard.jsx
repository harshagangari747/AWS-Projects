export default function ArticleCard({ article }) {
  return (
    <div className="card">
      <h2 className="title">{article.headline}</h2>
      <p className="eyebrow">{article.eyebrow}</p>

      <p className="summary">{article.summary}</p>

      <p className="authors">{article.authors.join(", ")}</p>

      <div className="footer">
        <a
          href={article.articleUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="full-link"
        >
          Full Article →
        </a>
      </div>
    </div>
  );
}
