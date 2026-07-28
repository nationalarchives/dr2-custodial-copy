package uk.gov.nationalarchives.confirmer

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

trait ScoutAmHttpService:
  def get(request: HttpRequest): HttpResponse[String]
  def post(request: HttpRequest): HttpResponse[String]

object ScoutAmHttpService:
  def apply(client: HttpClient): ScoutAmHttpService = new ScoutAmHttpService:
    override def get(request: HttpRequest): HttpResponse[String] =
      client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString())

    override def post(request: HttpRequest): HttpResponse[String] =
      client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString())
