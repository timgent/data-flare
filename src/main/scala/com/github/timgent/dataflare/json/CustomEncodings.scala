package com.github.timgent.dataflare.json

import com.github.timgent.dataflare.checks.DatasourceDescription
import com.github.timgent.dataflare.checks.DatasourceDescription.{DualDsDescription, OtherDsDescription, SingleDsDescription}
import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}
import cats.syntax.either._

private[dataflare] object CustomEncodings {
  implicit val singleDsDescriptionEncoder: Encoder[SingleDsDescription] = new Encoder[SingleDsDescription] {
    override def apply(a: SingleDsDescription): Json = ???
  }

}
