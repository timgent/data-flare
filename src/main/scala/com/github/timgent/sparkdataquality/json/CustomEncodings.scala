package com.github.timgent.sparkdataquality.json

import com.github.timgent.sparkdataquality.checks.DatasourceDescription
import com.github.timgent.sparkdataquality.checks.DatasourceDescription.{DualDsDescription, OtherDsDescription, SingleDsDescription}
import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}
import cats.syntax.either._

private[sparkdataquality] object CustomEncodings {
  implicit val singleDsDescriptionEncoder: Encoder[SingleDsDescription] = new Encoder[SingleDsDescription] {
    override def apply(a: SingleDsDescription): Json = ???
  }

}
