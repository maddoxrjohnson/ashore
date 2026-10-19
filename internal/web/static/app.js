// Reveal buttons swap a config var's mask for its value and back.
document.querySelectorAll("button.reveal").forEach(function (b) {
  b.addEventListener("click", function () {
    var span = b.closest("tr").querySelector(".masked");
    var shown = span.dataset.shown === "1";
    span.textContent = shown ? "••••••••" : span.dataset.value;
    span.dataset.shown = shown ? "0" : "1";
    b.textContent = shown ? "reveal" : "hide";
  });
});

// The log view follows the app's stream and keeps the last 500 lines.
var pre = document.getElementById("logs");
if (pre) {
  var state = document.getElementById("logs-state");
  var es = new EventSource("/apps/" + pre.dataset.app + "/logs");
  es.addEventListener("open", function () { state.textContent = "live"; });
  es.addEventListener("error", function () { state.textContent = "reconnecting"; });
  es.addEventListener("done", function () { state.textContent = "ended"; es.close(); });
  es.addEventListener("line", function (e) {
    var l = JSON.parse(e.data);
    var div = document.createElement("div");
    div.className = l.stream;
    var who = l.stream === "ashore" ? "ashore" : l.release;
    div.textContent = new Date(l.time).toLocaleTimeString() + " " + who + "[" + l.stream + "]: " + l.text;
    var atBottom = pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 4;
    pre.appendChild(div);
    while (pre.childElementCount > 500) pre.removeChild(pre.firstChild);
    if (atBottom) pre.scrollTop = pre.scrollHeight;
  });
}
