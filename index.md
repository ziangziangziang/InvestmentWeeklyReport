---
title: Home
---

Welcome. Browse the weekly forsee and daily reports below.

## Weekly Forsee
<ul>
{% assign weeks = site.pages | where_exp: "p", "p.path contains 'forsee/'" | sort: "path" | reverse %}
{% for p in weeks %}
  {% unless p.name == "index.md" %}
  <li>
    <a href="{{ p.url | relative_url }}">
      {{ p.path | split: "/" | last | split: "." | first }}
    </a>
  </li>
  {% endunless %}
{% endfor %}
</ul>

## Daily Report
<ul>
{% assign days = site.pages | where_exp: "p", "p.path contains 'report/'" | sort: "path" | reverse %}
{% for p in days %}
  {% unless p.name == "index.md" %}
  <li>
    <a href="{{ p.url | relative_url }}">
      {{ p.path | split: "/" | last | split: "." | first }}
    </a>
  </li>
  {% endunless %}
{% endfor %}
</ul>
