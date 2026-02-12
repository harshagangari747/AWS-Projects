import { useEffect, useState } from "react";
import ArticleCard from "./Components/ArticleCard";
import Pagination from "./Components/Pagination";
import { fetchArticles } from "./api/fetchArticles";
import "./styles.css";

function App() {
  const today = new Date().toISOString().split("T")[0];

  const [date, setDate] = useState(today);
  const [page, setPage] = useState(1);
  const [articles, setArticles] = useState([]);
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState("home");
  const [menuOpen, setMenuOpen] = useState(false);
  const [hasMore, setHasMore] = useState(true);
  const [message, setMessage] = useState("");

  const loadData = async () => {
    try {
      setLoading(true);
      const data = await fetchArticles(date, page);
      const fetchedArticles = data.articles || [];

      if (fetchedArticles.length > 0) {
        setArticles(fetchedArticles);
        setMessage(""); // clear previous messages
      } else if (page > 1) {
        setArticles([]);
        setMessage("You have reached all the articles for this date!");
      } else {
        setArticles([]);
        setMessage(`No summaries found for the date: ${date}`);
      }
    } catch (err) {
      setArticles([]);
      setMessage(`Failed to fetch articles for ${date}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (activeTab === "home") {
      loadData();
    }
  }, [date, page, activeTab]);

  return (
    <div>
      {/* NAVBAR */}
      <nav className="navbar">
        <div className="nav-left">
          <h1 className="logo">
            <span className="arxiv">Arxiv</span>
            <span className="shorts">Shorts</span>
          </h1>
        </div>

        {/* Desktop Nav */}
        <div className="nav-right desktop-nav">
          <button
            className={activeTab === "home" ? "nav-btn active" : "nav-btn"}
            onClick={() => setActiveTab("home")}
          >
            Home
          </button>
          <button
            className={activeTab === "about" ? "nav-btn active" : "nav-btn"}
            onClick={() => setActiveTab("about")}
          >
            About
          </button>
          <button
            className={activeTab === "developer" ? "nav-btn active" : "nav-btn"}
            onClick={() => setActiveTab("developer")}
          >
            Developer
          </button>
        </div>

        {/* Hamburger */}
        <div className="hamburger" onClick={() => setMenuOpen(!menuOpen)}>
          ☰
        </div>
      </nav>

      {/* Mobile Dropdown */}
      {menuOpen && (
        <div className="mobile-menu">
          <button
            className="mobile-nav-btn"
            onClick={() => {
              setActiveTab("home");
              setMenuOpen(false);
            }}
          >
            Home
          </button>
          <button
            className="mobile-nav-btn"
            onClick={() => {
              setActiveTab("about");
              setMenuOpen(false);
            }}
          >
            About
          </button>
          <button
            className="mobile-nav-btn"
            onClick={() => {
              setActiveTab("developer");
              setMenuOpen(false);
            }}
          >
            Developer
          </button>
        </div>
      )}

      <div className="container">
        {/* DATE PICKER BELOW NAV */}
        {activeTab === "home" && (
          <div className="date-row">
            <input
              type="date"
              value={date}
              onChange={(e) => {
                setPage(1);
                setDate(e.target.value);
              }}
              className="date-picker"
            />
          </div>
        )}

        {/* HOME CONTENT */}
        {activeTab === "home" && (
          <>
            {loading ? (
              <div className="loading">Loading summaries...</div>
            ) : articles.length > 0 ? (
              <>
                {articles.map((article) => (
                  <ArticleCard key={article.paper_id} article={article} />
                ))}

                <Pagination currentPage={page} onPageChange={setPage} />
              </>
            ) : (
              <div className="error-card">
                <h2>{message}</h2>
                <div className="sad">😔</div>
                <Pagination currentPage={page} onPageChange={setPage} />
              </div>
            )}
          </>
        )}

        {/* ABOUT SECTION */}
        {activeTab === "about" && (
          <div className="info-card">
            <h2>About ArxivShorts</h2>
            <p>
              <b>ArxivShorts</b> is a concise research news platform inspired by
              India's Way2News 📰 app. It allows you to quickly skim through
              recently published research papers from the computer science
              domain.
              <br></br>
              <br></br>
              <b>Background</b>
              <p>
                {" "}
                There are many applications that let you interact with research
                papers, such as RAG models or other summarization tools 🤖.
                However, most of these require you to first provide a reference
                to the article 📄 before they can summarize it. This means you
                have to search for papers and manually feed them into the
                summarizer 🔍. <br></br>
                <b>ArxivShorts</b> does the heavy lifting for you 💪. It keeps
                you informed about the latest research papers 🧪🚀 and provides
                concise, high-quality summaries ✨ so you can stay updated
                without any extra effort!
              </p>
              <br></br>
              <br></br>
              <b>Powered By</b>
              <p>
                Every day, the latest 100 research articles are collected and
                carefully summarized using the <br></br>{" "}
                <b>Claude Haiku 3.5 model on Amazon Bedrock</b>.
              </p>
              <br></br>
              <br></br>
              <b>Goal</b>
              <p>
                Our goal is simple: Help you stay informed about cutting-edge
                research without spending hours navigating dense academic
                papers. ArxivShorts delivers clarity, context, and insight so
                you can keep up with the research world efficiently and
                intelligently 🧠⚡.
              </p>
            </p>
          </div>
        )}

        {/* DEVELOPER SECTION */}
        {activeTab === "developer" && (
          <div className="info-card">
            <h2>Developer</h2>
            <br></br>
            <p>
              <b>About Me</b>
              <p>
                Hello, I am <b>Sai Harsha Gangari</b>. I am a grad student at
                George Washington University, graduate with a Master of Science
                in Computer Science degree. I am a software developer with a
                total experience of 3+ years. I love building prototypes and
                model them for fun. I have technical and deep understanding of
                AWS services.
              </p>
              <br></br>
              <br></br>
              <b>Technology</b>
              <p>
                This project was completely hosted on AWS serverless services.
                Key services include Lambda, Simple Queue Service, DynamoDB,
                DynamoDB Streams, Api gateway and Amazon Bedrock. The frontend
                is hosted on netlify
              </p>
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
