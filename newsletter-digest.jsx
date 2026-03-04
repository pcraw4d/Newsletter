import { useState } from "react";

const mockData = {
  date: "Wednesday, March 4, 2026",
  synthesis: {
    themes: [
      {
        id: 1,
        tag: "MACRO SIGNAL",
        title: "Fed policy uncertainty is reshaping fintech credit strategies",
        summary:
          "Three separate newsletters converged on the same signal this week: fintechs are quietly pulling back on BNPL and unsecured credit products ahead of an expected rate hold. The Generalist and Net Interest both cited tightening charge-off data; Fintech Blueprint pointed to Affirm's revised guidance as the canary.",
        sources: ["The Generalist", "Net Interest", "Fintech Blueprint"],
        articles: 5,
        confidence: "HIGH",
      },
      {
        id: 2,
        tag: "PRODUCT TREND",
        title: "AI compliance tooling is becoming a boardroom conversation",
        summary:
          "Regulatory pressure from the EU AI Act is accelerating enterprise demand for explainable AI in risk workflows. Lenny's Newsletter covered how PMs are being asked to own compliance artifacts, while a16z's newsletter highlighted a wave of Series A deals in this exact wedge.",
        sources: ["Lenny's Newsletter", "a16z Newsletter"],
        articles: 3,
        confidence: "MEDIUM",
      },
      {
        id: 3,
        tag: "EMERGING",
        title: "Embedded KYB is eating standalone identity verification",
        summary:
          "Multiple founders in the Stacked Marketer and Indie Hackers digests referenced switching from point solutions to embedded orchestration layers. The pain point cited repeatedly: too many vendor portals, too little signal consolidation.",
        sources: ["Stacked Marketer", "Indie Hackers"],
        articles: 2,
        confidence: "MEDIUM",
      },
    ],
  },
  newsletters: [
    {
      id: 1,
      sender: "Net Interest",
      author: "Marc Rubinstein",
      receivedAt: "6:14 AM",
      subject: "The Credit Cycle Turns",
      category: "Fintech & Markets",
      color: "#E8C547",
      takeaways: [
        "Charge-off rates at top 10 consumer fintechs rose 40bps QoQ — the steepest quarterly move since 2020",
        "Rubinstein argues this is structural, not cyclical: underwriting models trained on 2021-2023 data are systematically miscalibrated",
        "Goldman's Marcus wind-down is now being cited as prescient rather than a strategic failure",
      ],
      articles: [
        {
          title: "Affirm Q4 Earnings Breakdown",
          url: "#",
          summary:
            "Delinquency rates ticked up 60bps but management guided to stability. The market is skeptical — stock fell 8% post-earnings. Key risk: merchant concentration in travel and home goods.",
          readTime: "6 min",
        },
        {
          title: "The Case for Secured Lending in a High-Rate World",
          url: "#",
          summary:
            "Auto-secured and invoice-backed products are seeing a quiet renaissance. Three neobanks pivoting away from unsecured personal loans toward asset-backed products.",
          readTime: "9 min",
        },
      ],
    },
    {
      id: 2,
      sender: "Lenny's Newsletter",
      author: "Lenny Rachitsky",
      receivedAt: "7:02 AM",
      subject: "How the best PMs handle AI product risk",
      category: "Product",
      color: "#5B8FF9",
      takeaways: [
        "Top PMs at AI-first companies are now owning compliance review cycles — a role that didn't exist 18 months ago",
        "The emerging PM archetype: 'Technical Enough to Prompt, Paranoid Enough to Ship Carefully'",
        "Lenny's survey data: 67% of PMs say AI features have increased their documentation burden 2x or more",
      ],
      articles: [
        {
          title: "Inside OpenAI's Product Review Process",
          url: "#",
          summary:
            "A rare look at how AI features get approved internally. Notable: every launch now requires a 'misuse scenario doc' signed off by a safety team before engineering starts.",
          readTime: "12 min",
        },
      ],
    },
    {
      id: 3,
      sender: "The Generalist",
      author: "Mario Gabriele",
      receivedAt: "8:30 AM",
      subject: "The Infrastructure Boom Nobody Is Talking About",
      category: "Venture & Tech",
      color: "#52C41A",
      takeaways: [
        "Gabriele argues we're in a 'picks and shovels' phase of AI — the real money is in data pipelines, not model providers",
        "Compliance data infrastructure is specifically called out: $4B+ deployed in 2025, with 2026 tracking to double",
        "The overlooked winner: companies that can make unstructured regulatory documents machine-readable",
      ],
      articles: [
        {
          title: "Who Wins the AI Data Wars",
          url: "#",
          summary:
            "Deep analysis of 40+ infrastructure startups. The thesis: proprietary training data is the moat, not model architecture. Companies with network-effect data flywheels will compound.",
          readTime: "18 min",
        },
        {
          title: "Regulatory Data as a Product Category",
          url: "#",
          summary:
            "Emerging category of companies turning compliance filings, court records, and sanction lists into structured APIs. LegitScript, Sayari, and three stealth startups featured.",
          readTime: "11 min",
        },
      ],
    },
    {
      id: 4,
      sender: "Fintech Blueprint",
      author: "Lex Sokolin",
      receivedAt: "9:15 AM",
      subject: "Agent-Native Financial Services",
      category: "Fintech & AI",
      color: "#FF6B6B",
      takeaways: [
        "Sokolin coins 'agent-native' — products designed from the ground up for AI agent consumption, not human UIs",
        "KYB and AML workflows are the first enterprise category going agent-native, driven by speed and cost pressure",
        "Prediction: by 2027, 40% of B2B fintech API calls will originate from AI agents, not human-driven applications",
      ],
      articles: [
        {
          title: "Stripe's Agent Toolkit Launch Analysis",
          url: "#",
          summary:
            "Deep dive into what Stripe's developer-facing AI tools mean for the ecosystem. Key insight: they're betting payments become a background service that agents invoke, not a checkout moment.",
          readTime: "14 min",
        },
      ],
    },
  ],
};

const confidenceColors = { HIGH: "#52C41A", MEDIUM: "#E8C547", LOW: "#FF6B6B" };

export default function App() {
  const [activeView, setActiveView] = useState("digest");
  const [expandedNewsletter, setExpandedNewsletter] = useState(null);
  const [expandedArticle, setExpandedArticle] = useState(null);
  const [expandedTheme, setExpandedTheme] = useState(null);

  return (
    <div style={{
      fontFamily: "'Georgia', 'Times New Roman', serif",
      background: "#0C0C0E",
      minHeight: "100vh",
      color: "#E8E4DC",
      padding: "0",
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;0,700;1,400&family=DM+Mono:wght@300;400&display=swap');

        * { box-sizing: border-box; margin: 0; padding: 0; }

        .header { 
          border-bottom: 1px solid #2A2A2E; 
          padding: 20px 40px;
          display: flex;
          align-items: center;
          justify-content: space-between;
          position: sticky;
          top: 0;
          background: #0C0C0E;
          z-index: 100;
        }

        .wordmark {
          font-family: 'Playfair Display', serif;
          font-size: 22px;
          letter-spacing: -0.5px;
          color: #E8E4DC;
        }

        .wordmark span {
          color: #E8C547;
        }

        .nav-tabs {
          display: flex;
          gap: 4px;
          background: #161618;
          border: 1px solid #2A2A2E;
          border-radius: 8px;
          padding: 4px;
        }

        .nav-tab {
          padding: 7px 18px;
          border-radius: 5px;
          font-family: 'DM Mono', monospace;
          font-size: 11px;
          letter-spacing: 0.08em;
          cursor: pointer;
          border: none;
          text-transform: uppercase;
          transition: all 0.15s;
          color: #666;
          background: transparent;
        }

        .nav-tab.active {
          background: #E8C547;
          color: #0C0C0E;
          font-weight: 400;
        }

        .date-badge {
          font-family: 'DM Mono', monospace;
          font-size: 11px;
          color: #555;
          letter-spacing: 0.06em;
          text-transform: uppercase;
        }

        .main { padding: 40px; max-width: 1100px; margin: 0 auto; }

        .section-label {
          font-family: 'DM Mono', monospace;
          font-size: 10px;
          letter-spacing: 0.15em;
          text-transform: uppercase;
          color: #E8C547;
          margin-bottom: 20px;
          display: flex;
          align-items: center;
          gap: 10px;
        }

        .section-label::after {
          content: '';
          flex: 1;
          height: 1px;
          background: #2A2A2E;
        }

        .theme-card {
          border: 1px solid #2A2A2E;
          border-radius: 10px;
          padding: 24px;
          margin-bottom: 14px;
          cursor: pointer;
          transition: all 0.2s;
          background: #111113;
          position: relative;
          overflow: hidden;
        }

        .theme-card::before {
          content: '';
          position: absolute;
          top: 0; left: 0;
          width: 3px;
          height: 100%;
          background: #E8C547;
        }

        .theme-card:hover { border-color: #444; background: #141416; }

        .theme-tag {
          font-family: 'DM Mono', monospace;
          font-size: 9px;
          letter-spacing: 0.15em;
          padding: 3px 8px;
          border-radius: 3px;
          background: #1E1E20;
          color: #E8C547;
          display: inline-block;
          margin-bottom: 10px;
        }

        .theme-title {
          font-family: 'Playfair Display', serif;
          font-size: 18px;
          line-height: 1.4;
          margin-bottom: 12px;
          color: #F0EBE0;
        }

        .theme-summary {
          font-size: 14px;
          line-height: 1.7;
          color: #999;
          display: none;
          margin-bottom: 14px;
        }

        .theme-card.expanded .theme-summary { display: block; }

        .theme-meta {
          display: flex;
          align-items: center;
          gap: 16px;
          flex-wrap: wrap;
        }

        .source-pill {
          font-family: 'DM Mono', monospace;
          font-size: 10px;
          color: #666;
          background: #1A1A1C;
          border: 1px solid #2A2A2E;
          padding: 3px 8px;
          border-radius: 20px;
        }

        .confidence-dot {
          width: 6px;
          height: 6px;
          border-radius: 50%;
          display: inline-block;
          margin-right: 5px;
        }

        .confidence-label {
          font-family: 'DM Mono', monospace;
          font-size: 10px;
          color: #555;
        }

        .divider { height: 1px; background: #1E1E20; margin: 40px 0; }

        .newsletter-row {
          border: 1px solid #222224;
          border-radius: 10px;
          margin-bottom: 12px;
          overflow: hidden;
          transition: border-color 0.2s;
          background: #0F0F11;
        }

        .newsletter-row:hover { border-color: #333; }

        .newsletter-header {
          padding: 20px 24px;
          display: flex;
          align-items: flex-start;
          gap: 16px;
          cursor: pointer;
        }

        .sender-dot {
          width: 36px;
          height: 36px;
          border-radius: 8px;
          display: flex;
          align-items: center;
          justify-content: center;
          font-family: 'DM Mono', monospace;
          font-size: 11px;
          font-weight: 400;
          flex-shrink: 0;
          color: #0C0C0E;
        }

        .newsletter-meta { flex: 1; min-width: 0; }

        .sender-name {
          font-family: 'Playfair Display', serif;
          font-size: 15px;
          color: #E8E4DC;
          margin-bottom: 2px;
        }

        .newsletter-subject {
          font-size: 12px;
          color: #666;
          font-style: italic;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }

        .newsletter-right {
          display: flex;
          flex-direction: column;
          align-items: flex-end;
          gap: 6px;
          flex-shrink: 0;
        }

        .time-label {
          font-family: 'DM Mono', monospace;
          font-size: 10px;
          color: #444;
        }

        .category-tag {
          font-family: 'DM Mono', monospace;
          font-size: 9px;
          letter-spacing: 0.08em;
          padding: 2px 8px;
          border-radius: 3px;
          background: #1A1A1C;
          color: #666;
          white-space: nowrap;
        }

        .newsletter-body {
          padding: 0 24px 24px 76px;
          display: none;
        }

        .newsletter-row.expanded .newsletter-body { display: block; }

        .takeaway-list { margin-bottom: 24px; }

        .takeaway-item {
          display: flex;
          gap: 10px;
          padding: 10px 0;
          border-bottom: 1px solid #1A1A1C;
          font-size: 13.5px;
          line-height: 1.6;
          color: #C0BAB0;
        }

        .takeaway-item:last-child { border-bottom: none; }

        .takeaway-bullet {
          color: #E8C547;
          flex-shrink: 0;
          margin-top: 3px;
          font-size: 10px;
        }

        .articles-label {
          font-family: 'DM Mono', monospace;
          font-size: 9px;
          letter-spacing: 0.12em;
          text-transform: uppercase;
          color: #444;
          margin-bottom: 10px;
        }

        .article-card {
          border: 1px solid #1E1E20;
          border-radius: 7px;
          padding: 14px 16px;
          margin-bottom: 8px;
          cursor: pointer;
          transition: all 0.15s;
          background: #0C0C0E;
        }

        .article-card:hover { border-color: #333; }

        .article-title {
          font-size: 13px;
          color: #D8D4CA;
          margin-bottom: 4px;
          display: flex;
          justify-content: space-between;
          align-items: flex-start;
          gap: 10px;
        }

        .article-read-time {
          font-family: 'DM Mono', monospace;
          font-size: 9px;
          color: #444;
          white-space: nowrap;
          flex-shrink: 0;
          margin-top: 2px;
        }

        .article-summary {
          font-size: 12px;
          color: #666;
          line-height: 1.65;
          display: none;
          margin-top: 10px;
          padding-top: 10px;
          border-top: 1px solid #1E1E20;
        }

        .article-card.expanded .article-summary { display: block; }

        .article-extracted-badge {
          font-family: 'DM Mono', monospace;
          font-size: 9px;
          color: #52C41A;
          display: flex;
          align-items: center;
          gap: 4px;
          margin-top: 8px;
        }

        .stats-bar {
          display: flex;
          gap: 24px;
          padding: 14px 0;
          border-bottom: 1px solid #1E1E20;
          margin-bottom: 32px;
        }

        .stat {
          display: flex;
          flex-direction: column;
          gap: 2px;
        }

        .stat-value {
          font-family: 'Playfair Display', serif;
          font-size: 26px;
          color: #E8E4DC;
        }

        .stat-label {
          font-family: 'DM Mono', monospace;
          font-size: 10px;
          color: #444;
          letter-spacing: 0.06em;
          text-transform: uppercase;
        }

        .expand-icon {
          color: #444;
          font-size: 12px;
          transition: transform 0.2s;
          margin-left: auto;
          flex-shrink: 0;
          align-self: center;
        }

        .newsletter-row.expanded .expand-icon { transform: rotate(180deg); }
        .theme-card.expanded .expand-icon { transform: rotate(180deg); }

        .empty-state {
          text-align: center;
          padding: 60px 20px;
          color: #444;
          font-family: 'DM Mono', monospace;
          font-size: 12px;
        }
      `}</style>

      {/* Header */}
      <div className="header">
        <div className="wordmark">Brief<span>ly</span></div>
        <div className="nav-tabs">
          {["digest", "newsletters", "trends"].map((v) => (
            <button
              key={v}
              className={`nav-tab ${activeView === v ? "active" : ""}`}
              onClick={() => setActiveView(v)}
            >
              {v}
            </button>
          ))}
        </div>
        <div className="date-badge">{mockData.date}</div>
      </div>

      <div className="main">
        {/* Stats Bar */}
        <div className="stats-bar">
          <div className="stat">
            <span className="stat-value">{mockData.newsletters.length}</span>
            <span className="stat-label">Newsletters</span>
          </div>
          <div className="stat">
            <span className="stat-value">{mockData.newsletters.reduce((a, n) => a + n.articles.length, 0)}</span>
            <span className="stat-label">Articles Extracted</span>
          </div>
          <div className="stat">
            <span className="stat-value">{mockData.synthesis.themes.length}</span>
            <span className="stat-label">Cross-Source Themes</span>
          </div>
          <div className="stat">
            <span className="stat-value">{mockData.newsletters.reduce((a, n) => a + n.takeaways.length, 0)}</span>
            <span className="stat-label">Key Takeaways</span>
          </div>
        </div>

        {/* DIGEST VIEW */}
        {activeView === "digest" && (
          <div>
            <div className="section-label">Intelligence Synthesis · {mockData.synthesis.themes.length} themes identified</div>

            {mockData.synthesis.themes.map((theme) => (
              <div
                key={theme.id}
                className={`theme-card ${expandedTheme === theme.id ? "expanded" : ""}`}
                onClick={() => setExpandedTheme(expandedTheme === theme.id ? null : theme.id)}
              >
                <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12 }}>
                  <div style={{ flex: 1 }}>
                    <div className="theme-tag">{theme.tag}</div>
                    <div className="theme-title">{theme.title}</div>
                  </div>
                  <div className="expand-icon">▼</div>
                </div>
                <div className="theme-summary">{theme.summary}</div>
                <div className="theme-meta">
                  {theme.sources.map((s) => (
                    <span key={s} className="source-pill">{s}</span>
                  ))}
                  <span style={{ marginLeft: "auto" }}>
                    <span className="confidence-dot" style={{ background: confidenceColors[theme.confidence] }}></span>
                    <span className="confidence-label">{theme.confidence} CONFIDENCE</span>
                  </span>
                </div>
              </div>
            ))}

            <div className="divider" />
            <div className="section-label">Per-Newsletter Summaries · Today</div>

            {mockData.newsletters.map((n) => (
              <div
                key={n.id}
                className={`newsletter-row ${expandedNewsletter === n.id ? "expanded" : ""}`}
              >
                <div
                  className="newsletter-header"
                  onClick={() => setExpandedNewsletter(expandedNewsletter === n.id ? null : n.id)}
                >
                  <div className="sender-dot" style={{ background: n.color }}>
                    {n.sender.charAt(0)}
                  </div>
                  <div className="newsletter-meta">
                    <div className="sender-name">{n.sender}</div>
                    <div className="newsletter-subject">"{n.subject}"</div>
                  </div>
                  <div className="newsletter-right">
                    <span className="time-label">{n.receivedAt}</span>
                    <span className="category-tag">{n.category}</span>
                  </div>
                  <div className="expand-icon">▼</div>
                </div>

                <div className="newsletter-body">
                  <div className="takeaway-list">
                    {n.takeaways.map((t, i) => (
                      <div key={i} className="takeaway-item">
                        <span className="takeaway-bullet">◆</span>
                        <span>{t}</span>
                      </div>
                    ))}
                  </div>

                  <div className="articles-label">Linked articles · extracted & summarized</div>
                  {n.articles.map((a, i) => {
                    const articleKey = `${n.id}-${i}`;
                    return (
                      <div
                        key={i}
                        className={`article-card ${expandedArticle === articleKey ? "expanded" : ""}`}
                        onClick={(e) => {
                          e.stopPropagation();
                          setExpandedArticle(expandedArticle === articleKey ? null : articleKey);
                        }}
                      >
                        <div className="article-title">
                          <span>{a.title}</span>
                          <span className="article-read-time">{a.readTime} read</span>
                        </div>
                        <div className="article-summary">{a.summary}</div>
                        <div className="article-extracted-badge">
                          <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#52C41A", display: "inline-block" }}></span>
                          Full article extracted & analyzed
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* NEWSLETTERS VIEW */}
        {activeView === "newsletters" && (
          <div>
            <div className="section-label">All Sources · {mockData.newsletters.length} active</div>
            {mockData.newsletters.map((n) => (
              <div key={n.id} style={{
                border: "1px solid #222224",
                borderRadius: 10,
                padding: "18px 24px",
                marginBottom: 10,
                display: "flex",
                alignItems: "center",
                gap: 16,
                background: "#0F0F11",
              }}>
                <div className="sender-dot" style={{ background: n.color }}>{n.sender.charAt(0)}</div>
                <div style={{ flex: 1 }}>
                  <div className="sender-name" style={{ fontSize: 14 }}>{n.sender}</div>
                  <div style={{ fontFamily: "'DM Mono', monospace", fontSize: 10, color: "#555", marginTop: 2 }}>by {n.author}</div>
                </div>
                <div style={{ textAlign: "right" }}>
                  <div className="category-tag">{n.category}</div>
                  <div style={{ fontFamily: "'DM Mono', monospace", fontSize: 10, color: "#444", marginTop: 6 }}>
                    {n.articles.length} articles extracted today
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* TRENDS VIEW */}
        {activeView === "trends" && (
          <div>
            <div className="section-label">Recurring Signals · Last 7 days</div>
            {[
              { topic: "AI in compliance workflows", count: 8, trend: "↑", newsletters: ["Fintech Blueprint", "The Generalist", "a16z"] },
              { topic: "Credit cycle deterioration", count: 6, trend: "↑", newsletters: ["Net Interest", "Fintech Blueprint"] },
              { topic: "Embedded finance consolidation", count: 5, trend: "→", newsletters: ["Fintech Blueprint", "Stacked Marketer"] },
              { topic: "Stripe ecosystem expansion", count: 4, trend: "↑", newsletters: ["The Generalist", "Indie Hackers"] },
              { topic: "EU AI Act implementation", count: 4, trend: "↑", newsletters: ["Lenny's Newsletter", "The Generalist"] },
            ].map((t, i) => (
              <div key={i} style={{
                border: "1px solid #222224",
                borderRadius: 10,
                padding: "20px 24px",
                marginBottom: 10,
                background: "#0F0F11",
                display: "flex",
                alignItems: "center",
                gap: 20,
              }}>
                <div style={{ fontFamily: "'Playfair Display', serif", fontSize: 28, color: "#E8C547", minWidth: 40 }}>
                  {t.trend}
                </div>
                <div style={{ flex: 1 }}>
                  <div style={{ fontSize: 15, color: "#E8E4DC", marginBottom: 8, fontFamily: "'Playfair Display', serif" }}>{t.topic}</div>
                  <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                    {t.newsletters.map((n) => (
                      <span key={n} className="source-pill">{n}</span>
                    ))}
                  </div>
                </div>
                <div style={{ textAlign: "right", flexShrink: 0 }}>
                  <div style={{ fontFamily: "'Playfair Display', serif", fontSize: 28, color: "#E8E4DC" }}>{t.count}</div>
                  <div style={{ fontFamily: "'DM Mono', monospace", fontSize: 10, color: "#444" }}>mentions</div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
