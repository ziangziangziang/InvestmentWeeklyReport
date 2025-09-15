---
title: Home
---

Welcome. Browse the weekly foresee and daily reports below.

## Weekly Foresee

### Latest

{% assign weeks = site.pages | where_exp: "p", "p.path contains 'foresee/'" | sort: "path" | reverse %}
{% assign latest_week = weeks | where_exp: "p", "p.name != 'index.md'" | first %}
{% if latest_week %}

  {% if latest_week.date %} — <small>{{ latest_week.date | date: "%Y-%m-%d" }}</small>{% endif %}

  <div class="embedded-report">
  {% comment %} Render the latest week's markdown inline. Use markdownify to convert Markdown to HTML. {% endcomment %}
  {{ latest_week.content | markdownify }}
  </div>

  <details>
    <summary>Show raw report page</summary>
    <p><a href="{{ latest_week.url | relative_url }}">Open full week report</a></p>
  </details>
{% else %}
- No weekly reports found.
{% endif %}

<details>
  <summary>All weekly foresee</summary>
  <ul>
  {% for p in weeks %}
    {% unless p.name == "index.md" %}
    <li>
      <a href="{{ p.url | relative_url }}">{{ p.title | default: (p.path | split: "/" | last | split: "." | first) }}</a>
      {% if p.date %}<small> — {{ p.date | date: "%Y-%m-%d" }}</small>{% endif %}
    </li>
    {% endunless %}
  {% endfor %}
  </ul>
</details>

## Daily Summarize
### Latest
{% assign days = site.pages | where_exp: "p", "p.path contains 'report/'" | sort: "path" | reverse %}
{% assign latest_day = days | where_exp: "p", "p.name != 'index.md'" | first %}
{% if latest_day %}
- <strong>Latest:</strong> <a href="{{ latest_day.url | relative_url }}">{{ latest_day.title | default: (latest_day.path | split: "/" | last | split: "." | first) }}</a>
  {% if latest_day.date %} — <small>{{ latest_day.date | date: "%Y-%m-%d" }}</small>{% endif %}

  <div class="embedded-report">
  {{ latest_day.content | markdownify }}
  </div>

  <details>
    <summary>Show raw report page</summary>
    <p><a href="{{ latest_day.url | relative_url }}">Open full daily report</a></p>
  </details>
{% else %}
- No daily reports found.
{% endif %}

<details>
  <summary>All daily reports</summary>
  <ul>
  {% for p in days %}
    {% unless p.name == "index.md" %}
    <li>
      <a href="{{ p.url | relative_url }}">{{ p.title | default: (p.path | split: "/" | last | split: "." | first) }}</a>
      {% if p.date %}<small> — {{ p.date | date: "%Y-%m-%d" }}</small>{% endif %}
    </li>
    {% endunless %}
  {% endfor %}
  </ul>
</details>
