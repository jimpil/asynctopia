(ns asynctopia.protocols)

(defprotocol IBufferCapability
  (clone-empty [this])
  (snapshot [this]))
