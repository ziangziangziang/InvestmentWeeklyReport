# Invest Report Repo

This repository hosts weekly forsee reports and daily reports, published [here](https://ziangziangziang.github.io/InvestmentWeeklyReport/) via GitHub Pages.

Folder structure
- public/: static assets required by the site (e.g., mermaid.min.js)
- forsee/: weekly forsee reports (Markdown, named yyyymmdd.md)
- report/: daily reports (Markdown, named yyyymmdd.md)

Branch strategy
- main: production site (GitHub Pages publishes from this branch)
- dev: ongoing work; feature branches merge into dev, then dev merges to main when ready

How publishing works
- GitHub Pages is configured to “Deploy from a branch” (main). Jekyll renders Markdown using a default layout.
- Mermaid diagrams are rendered client-side with public/mermaid.min.js included in the default layout.
- No front matter is required in each Markdown file; the jekyll-optional-front-matter plugin and _config.yml defaults apply a layout automatically.

Preview the site locally with Jekyll (recommended)
We use the GitHub Pages Jekyll stack locally so the preview matches production.

1) One-time setup (macOS, Apple Silicon)
```sh
# Install rbenv (or use your preferred Ruby manager)
brew install rbenv ruby-build
rbenv init
source ~/.zshrc
rbenv install 3.3.4
rbenv local 3.3.4

# Install bundler and dependencies
gem install bundler
bundle install
```

2) Run the local server with live reload
```sh
bundle exec jekyll serve --livereload --config _config.yml,_config.local.yml
```
Then open http://127.0.0.1:4000 to preview the site. As you edit files, the page reloads automatically.

Notes
- Update _config.yml url and baseurl to your GitHub username/repo before publishing. For a project site at https://USER.github.io/REPO set:
  - url: "https://USER.github.io"
  - baseurl: "/REPO"
- Navigation and asset links use Liquid’s relative_url filter so they work both locally and on GitHub Pages.
- If you add new Mermaid diagrams, use fenced code blocks with ```mermaid; they render client-side.

Deploying updates
- Merge dev into main. GitHub Pages will rebuild automatically. Changes go live after the build completes.

Troubleshooting
- If Jekyll doesn’t start locally, ensure you’re using the Ruby version in .ruby-version and rerun bundle install.
- If diagrams don’t render, confirm public/mermaid.min.js exists and the layout includes it.
- If links look broken locally, ensure you started the server with the local override config (_config.local.yml).
