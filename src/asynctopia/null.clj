(ns asynctopia.null)

(defn replacing
  "If <x> is nil returns ::nil, otherwise <x>.
   Useful when putting (unknown) stuff into channels."
  [x]
  (if (nil? x) ::nil x))

(defonce null? (partial = ::nil))

(defn restoring
  "If <x> is ::nil returns nil, otherwise <x>.
   Useful when taking (potentially) nil-converted
   stuff from channels."
  [x]
  (when-not (null? x) x))
