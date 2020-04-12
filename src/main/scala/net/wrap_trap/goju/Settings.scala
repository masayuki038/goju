package net.wrap_trap.goju

import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory

/**
 * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

 * Copyright (c) 2016 Masayuki Takahashi

 * This software is released under the MIT License.
 * http://opensource.org/licenses/mit-license.php
 */
object Settings {
  var config: Option[Config] = None

  def getSettings: Settings = {
    if (config.isEmpty) {
      config = Option(ConfigFactory.load())
    }
    new Settings(config.get)
  }
}

class Settings(config: Config) {

  def getString(path: String, default: String): String =
    readValue(path, config.getString(path), default)

  def getInt(path: String, default: Int): Int = readValue(path, config.getInt(path), default)

  def getLong(path: String, default: Long): Long = readValue(path, config.getLong(path), default)

  def getBoolean(path: String, default: Boolean): Boolean =
    readValue(path, config.getBoolean(path), default)

  def hasPath(path: String): Boolean = config.hasPath(path)

  private def readValue[T](path: String, v: => T, default: T): T = {
    try {
      v
    } catch {
      case _: ConfigException.Missing => default
      case _: Throwable => throw new IllegalArgumentException("Failed to get: " + path)
    }
  }
}
