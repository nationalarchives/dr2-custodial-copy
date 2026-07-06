package uk.gov.nationalarchives.confirmer

import uk.gov.nationalarchives.confirmer.Confirmer.Result

trait Confirmer:
  def getResult(payload: Payload, confirmationOperator: ConfirmationOperator): Result

object Confirmer:

  enum Result:
    case Success(dynamoMap: Map[String, List[String]])
    case Failure(errorMessage: Throwable)

    def isSuccess: Boolean = this match
      case _: Success => true
      case _: Failure => false

    def isError: Boolean = !isSuccess

  def getConfirmer(config: Config): Confirmer =
    config.dynamoAttributeName match
      case "TC_result" => tcConfirmer
      case "CC_result" => ccConfirmer
      case _           => throw new IllegalArgumentException(s"Unable to create confirmer corresponding to ${config.dynamoAttributeName}")

  val ccConfirmer: Confirmer =
    (payload: Payload, confirmationOperator: ConfirmationOperator) => {
      payload match {
        case cc: CCPayload =>
          confirmationOperator match {
            case oper: CCOperator =>
              val objectFilePaths = oper.ocfl.getFilePathsForObject(cc.preservationSystemId)
              if objectFilePaths.nonEmpty then Result.Success(Map("filePaths" -> objectFilePaths))
              else Result.Failure(new RuntimeException(s"filePaths could not be found for ${cc.preservationSystemId.toString}"))
            case _ => Result.Failure(new RuntimeException(s"Unsupported operation in CC confirmer ${cc.preservationSystemId.toString}"))
          }
        case _ => Result.Failure(new IllegalArgumentException("Invalid payload type for CC confirmer"))
      }
    }

  val tcConfirmer: Confirmer =
    (payload: Payload, confirmationOperator: ConfirmationOperator) => {
      payload match {
        case tc: TCPayload =>
          confirmationOperator match {
            case operator: TCOperator =>
              val tapeConfirmation = operator.scoutAM.getFileDetails(tc.filePaths)
              if tapeConfirmation.nonEmpty then Result.Success(tapeConfirmation)
              else Result.Failure(new RuntimeException(s"Volumes could not be found for one or more files in ${tc.filePaths.mkString(", ")}"))
            case _ => Result.Failure(new RuntimeException("Unsupported operation in TC confirmer"))
          }
        case _ => Result.Failure(new IllegalArgumentException("Invalid payload type for TC confirmer"))
      }
    }
