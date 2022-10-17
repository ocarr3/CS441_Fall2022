package edu.uic.cs441
package HW1Utils

import HW1Utils.HW1CreateLogger

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.{Failure, Success, Try}

// Grab config file for for the application 
object HW1ObtainConfigReference:
  private val config = ConfigFactory.load("HW1Configs")
  private val logger = HW1CreateLogger(classOf[HW1ObtainConfigReference.type])

  private def ValidateConfig(confEntry: String): Boolean = Try(config.getConfig(confEntry)) match {
    case Failure(exception) => logger.error(s"Failed to retrieve config entry $confEntry for reason $exception"); false
    case Success(_) => true
  }

  def apply(confEntry: String): Option[Config] = if ValidateConfig(confEntry) then Some(config) else None