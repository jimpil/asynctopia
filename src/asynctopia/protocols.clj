(ns asynctopia.protocols
  (:import (asynctopia.protocols IEmptyBuffer ISnapshotBuffer)))

(defprotocol IEmptyBuffer
  (clone-empty [this]))

(defprotocol ISnapshotBuffer
  (snapshot [this]))
