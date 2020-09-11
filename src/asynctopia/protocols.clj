(ns asynctopia.protocols)

(defprotocol IEmpty
  (clone-empty [this]))

(defprotocol ISnapshot
  (snapshot [this]))
