/* VistaMirror 登录页海报墙
 * 从 /api/public/login-backdrop 拉取 TMDB 趋势海报（服务端缓存、Token 不下发），
 * 构建多列无限滚动背景墙。失败或无配置时静默保持渐变兜底背景。
 */
(function () {
  "use strict";

  var COLUMN_WIDTH = 240; // 估算列宽，用于计算列数

  function buildWall(shell, backdrop, posters) {
    var columnCount = Math.max(3, Math.min(6, Math.ceil(window.innerWidth / COLUMN_WIDTH)));

    // 海报轮转分配到各列
    var assignments = [];
    for (var i = 0; i < columnCount; i += 1) {
      assignments.push([]);
    }
    posters.forEach(function (poster, index) {
      assignments[index % columnCount].push(poster);
    });

    // 每列先铺完一整轮，再整体复制第二轮
    // （translateY(-50%) 无缝循环要求前后两半序列完全相同）
    assignments.forEach(function (postersInCol) {
      var col = document.createElement("div");
      col.className = "auth-backdrop-col";
      for (var copy = 0; copy < 2; copy += 1) {
        postersInCol.forEach(function (poster) {
          var figure = document.createElement("div");
          figure.className = "auth-backdrop-item";
          var img = document.createElement("img");
          img.src = poster.url;
          img.alt = "";
          img.loading = "eager";
          img.decoding = "async";
          img.addEventListener("error", function () {
            // 单张海报加载失败时静默移除，绝不留破图
            if (this.parentNode) {
              this.parentNode.remove();
            }
          });
          figure.appendChild(img);
          col.appendChild(figure);
        });
      }
      backdrop.appendChild(col);
    });

    shell.classList.add("auth-wall-on");
  }

  function renderFeatured(posters) {
    var featuredBox = document.getElementById("admin-auth-featured");
    if (!featuredBox) {
      return;
    }
    var rated = posters.filter(function (p) {
      return p && p.title && typeof p.rating === "number" && p.rating > 0;
    });
    if (!rated.length) {
      return;
    }
    // 每天从评分前 10 名里轮换一部，同一天内保持稳定
    rated.sort(function (a, b) { return b.rating - a.rating; });
    var top = rated.slice(0, 10);
    var now = new Date();
    var dayOfYear = Math.floor((now - new Date(now.getFullYear(), 0, 0)) / 86400000);
    var pick = top[dayOfYear % top.length];

    var posterImg = document.getElementById("admin-auth-featured-poster");
    var titleEl = document.getElementById("admin-auth-featured-title");
    var ratingEl = document.getElementById("admin-auth-featured-rating");
    if (posterImg) {
      posterImg.src = pick.url;
      posterImg.alt = pick.title;
    }
    if (titleEl) {
      titleEl.textContent = pick.title + (pick.mediaType === "tv" ? " · 剧集" : "");
    }
    if (ratingEl) {
      ratingEl.textContent = "★ " + pick.rating.toFixed(1);
    }
    featuredBox.hidden = false;
  }

  function init() {
    var shell = document.getElementById("admin-auth-shell");
    var backdrop = document.getElementById("auth-backdrop");
    if (!shell || !backdrop) {
      return;
    }
    fetch("/api/public/login-backdrop", { credentials: "same-origin" })
      .then(function (res) {
        if (!res.ok) {
          throw new Error("backdrop http " + res.status);
        }
        return res.json();
      })
      .then(function (payload) {
        var posters = payload && Array.isArray(payload.posters) ? payload.posters : [];
        posters = posters.filter(function (p) {
          // 只接受同源代理地址（防御异常 payload）
          return p && typeof p.url === "string" && p.url.indexOf("/api/public/login-backdrop/image?") === 0;
        });
        renderFeatured(posters);
        if (posters.length < 6) {
          return; // 海报太少不出墙，保持渐变兜底
        }
        buildWall(shell, backdrop, posters);
      })
      .catch(function () {
        /* 网络或服务端失败：静默保持渐变兜底 */
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
